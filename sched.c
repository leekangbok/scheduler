#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
#include <stdint.h>
#include <stdatomic.h>

/* ========================================================================= *
 * [1] 메모리 트래킹 엔진
 * ========================================================================= */
static atomic_int internal_alloc_count = 0;

void* tracker_malloc(size_t size) {
    if (size == 0) return NULL;
    atomic_fetch_add(&internal_alloc_count, 1);
    return malloc(size);
}

void tracker_free(void* ptr) {
    if (!ptr) return;
    free(ptr);
    atomic_fetch_sub(&internal_alloc_count, 1);
}

void* tracker_realloc(void* ptr, size_t size) {
    if (!ptr) return tracker_malloc(size);
    if (size == 0) { tracker_free(ptr); return NULL; }
    return realloc(ptr, size);
}

#define MALLOC tracker_malloc
#define REALLOC tracker_realloc
#define FREE tracker_free

/* ========================================================================= *
 * [2] 상수 및 자료구조
 * ========================================================================= */
#define MIN_WORKERS 4
#define MAX_WORKERS 20
#define IDLE_TIMEOUT_SEC 5
#define INITIAL_CAPACITY 16

typedef struct Scheduler Scheduler;
typedef struct TaskContext TaskContext;

typedef enum { POLICY_OVERLAP = 0, POLICY_SKIP = 1, POLICY_WAIT = 2 } OverrunPolicy;
typedef enum { TYPE_RELATIVE = 0, TYPE_CALENDAR = 1 } ScheduleType;
#define TIME_ANY -1
typedef enum {
	DAY_ANY = -1,
	DAY_SUN = 0,  // 일요일
	DAY_MON = 1,  // 월요일
	DAY_TUE = 2,  // 화요일
	DAY_WED = 3,  // 수요일
	DAY_THU = 4,  // 목요일
	DAY_FRI = 5,  // 금요일
	DAY_SAT = 6   // 토요일
} WeekDay;

typedef void (*TaskFunc)(TaskContext* ctx);

struct TaskContext {
    Scheduler* sched;
    void* user_arg;
    uint64_t task_id;
};

typedef struct JobNode {
    TaskFunc func; void* arg; uint64_t task_id;
    struct JobNode* next;
} JobNode;

typedef struct {
    uint64_t id;
    int is_active;     
    int is_periodic;
    ScheduleType type;
    struct timespec next_run;
    long interval_ms;
    int w, h, m;       
    int use_thread;
    int is_urgent;       
    OverrunPolicy policy;
    int is_running_now;
    TaskFunc func;
    void* arg;
} Task;

struct Scheduler {
    Task* tasks;
    int capacity;
    int task_count; 
    uint64_t next_task_id;
    pthread_mutex_t lock;
    volatile int is_shutting_down;
    pthread_t scheduler_thread;
    pthread_cond_t wakeup_cond;
    JobNode *job_head, *job_tail;
    pthread_cond_t job_cond;
    int cur_workers;
    int busy_workers;
    int active_jobs; 
    int queued_jobs; // ✅ [FIX 1] 큐에 쌓인 작업 개수를 추적하는 변수 추가
};

/* ========================================================================= *
 * [3] 유틸리티 및 시간 계산 (CLOCK_MONOTONIC 적용)
 * ========================================================================= */
struct timespec timespec_now_monotonic() {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return ts;
}
struct timespec timespec_add_ms(struct timespec ts, long ms) {
    ts.tv_sec += ms / 1000; ts.tv_nsec += (ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
    return ts;
}
int timespec_cmp(struct timespec* a, struct timespec* b) {
    if (a->tv_sec != b->tv_sec) return a->tv_sec < b->tv_sec ? -1 : 1;
    return (a->tv_nsec < b->tv_nsec) ? -1 : (a->tv_nsec > b->tv_nsec);
}

// [3] 유틸리티 영역의 캘린더 계산 함수 교체

struct timespec get_next_calendar_monotonic(int tw, int th, int tm) {
	time_t now_t = time(NULL);
	struct tm t;

	localtime_r(&now_t, &t);
	t.tm_sec = 0; // 초는 무조건 0 정각으로 맞춤

    // 1. 지정된 분/시간 적용
    if (tm >= 0) t.tm_min = tm;
    if (th >= 0) t.tm_hour = th;
    
    // 현재 기준으로 세팅된 시간 산출
    time_t next_t = mktime(&t);

    // 2. 만약 계산된 시간이 현재보다 과거라면 한 칸 전진!
    if (next_t <= now_t) {
        if (th < 0) {
            t.tm_hour++; // ANY 시간: 1시간 뒤로 (23시 -> 24시가 되면 mktime이 알아서 내일 0시로 바꿔줌)
        } else if (tw < 0) {
            t.tm_mday++; // 특정 시간 + ANY 요일: 내일 이 시간으로
        } else {
            t.tm_mday += 7; // 특정 시간 + 특정 요일: 다음 주 이 시간으로
        }
        next_t = mktime(&t); // 전진한 값으로 재계산
    }

    // 3. 요일(Weekday) 맞추기
    if (tw >= 0) {
        if (t.tm_wday != tw) {
            int diff = (tw - t.tm_wday + 7) % 7;
            t.tm_mday += diff; // 목표 요일만큼 날짜 점프!
            
            // 🚨 [엣지 케이스 완벽 해결] 
            // 다른 요일(미래)로 점프했는데 시간이 ANY(-1)라면, 그 날의 가장 빠른 0시로 리셋해야 함!
            if (th < 0) {
                t.tm_hour = 0;
            }
            next_t = mktime(&t);
        }
    }

    // 4. 스케줄러가 이해할 수 있는 Monotonic 대기 시간으로 변환
    long long delay_sec = (long long)(next_t - now_t);
    
    // [무한 루프 방지] 오차로 인해 0 이하가 나오면 강제로 60초 뒤로 밀어냄
    if (delay_sec <= 0) {
        delay_sec = 60;
    }

    struct timespec now_mt = timespec_now_monotonic();
    now_mt.tv_sec += delay_sec;
    return now_mt;
}

/* ========================================================================= *
 * [4] 슬롯 관리 및 코어 로직
 * ========================================================================= */
static int get_available_slot(Scheduler* s) {
    for (int i = 0; i < s->task_count; i++) if (!s->tasks[i].is_active && s->tasks[i].is_running_now == 0) return i;
    if (s->task_count >= s->capacity) { s->capacity *= 2; s->tasks = REALLOC(s->tasks, sizeof(Task) * s->capacity); }
    return s->task_count++;
}

Task* find_task(Scheduler* s, uint64_t id) {
    for (int i = 0; i < s->task_count; i++) if (s->tasks[i].id == id) return &s->tasks[i];
    return NULL;
}

// VIP 전용 워커 스레드 파라미터 구조체
typedef struct {
    Scheduler* sched; TaskFunc func; void* arg; uint64_t task_id;
} UrgentArgs;

void* urgent_worker_proc(void* arg) 
{
	UrgentArgs* uargs = (UrgentArgs*)arg;
	Scheduler* s = uargs->sched;
	uint64_t task_id = uargs->task_id;
	TaskContext ctx = { 
		.sched = s,
		.user_arg = uargs->arg,
		.task_id = task_id
	};
	uargs->func(&ctx);

	pthread_mutex_lock(&s->lock); s->active_jobs--; 
	Task* t = find_task(s, task_id);
	if (t) {
		t->is_running_now--;
	}
	if ((t && t->is_running_now == 0) || s->active_jobs == 0) {
		pthread_cond_broadcast(&s->wakeup_cond);
	}
	pthread_mutex_unlock(&s->lock);
	FREE(uargs);
	return NULL;
}

void* worker_proc(void* arg) {
    Scheduler* s = (Scheduler*)arg;
    while (1) {
        pthread_mutex_lock(&s->lock);
        
		while (s->job_head == NULL && !s->is_shutting_down) {
			struct timespec out = timespec_now_monotonic();
            out.tv_sec += IDLE_TIMEOUT_SEC;
			if (pthread_cond_timedwait(&s->job_cond, &s->lock, &out) == ETIMEDOUT) {
				if (s->cur_workers > MIN_WORKERS) { 
					s->cur_workers--; 
					pthread_mutex_unlock(&s->lock); 
					return NULL; 
				}
			}
		}
        
        // ✅ [FIX 2] 스케줄러 종료 명령이 떨어지면 남은 큐를 새로 집지 않고 즉시 퇴근!
        if (s->is_shutting_down) { s->cur_workers--; pthread_mutex_unlock(&s->lock); break; }
        
        JobNode* job = s->job_head; s->job_head = job->next;
        if (!s->job_head) s->job_tail = NULL;

		s->queued_jobs--; // 큐에서 꺼냈으므로 대기 카운트 감소
		s->busy_workers++; s->active_jobs++;
		pthread_mutex_unlock(&s->lock);

		TaskContext ctx = { .sched = s, .user_arg = job->arg, .task_id = job->task_id };
		job->func(&ctx);

        pthread_mutex_lock(&s->lock);
        s->busy_workers--; s->active_jobs--;
        Task* t = find_task(s, job->task_id); if (t) t->is_running_now--;
        FREE(job); 

        if ((t && t->is_running_now == 0) || s->active_jobs == 0) pthread_cond_broadcast(&s->wakeup_cond);
        pthread_mutex_unlock(&s->lock);
    }
    return NULL;
}

void* scheduler_loop(void* arg) {
    Scheduler* s = (Scheduler*)arg;
    while (!s->is_shutting_down) {
        pthread_mutex_lock(&s->lock);
        struct timespec now = timespec_now_monotonic();
        struct timespec next_w = {0, 0}; int has_t = 0;

        for (int i = 0; i < s->task_count; i++) {
            Task* t = &s->tasks[i];
            if (!t->is_active) continue;
            
            if (timespec_cmp(&t->next_run, &now) <= 0) {
                int run = 1;
				if (t->is_running_now > 0) {
					if (t->policy == POLICY_SKIP) {
						run = 0;
						t->next_run = (t->type == TYPE_RELATIVE) ? timespec_add_ms(t->next_run, t->interval_ms) : get_next_calendar_monotonic(t->w, t->h, t->m);
					} else if (t->policy == POLICY_WAIT) {
						run = 0;
					}
				}
                if (run) {
                    t->is_running_now++;
                    if (t->use_thread) {
                        if (t->is_urgent) {
                            s->active_jobs++; 
                            pthread_t tid;
                            UrgentArgs* uargs = MALLOC(sizeof(UrgentArgs));
                            uargs->sched = s; uargs->func = t->func; uargs->arg = t->arg; uargs->task_id = t->id;
                            
                            if (pthread_create(&tid, NULL, urgent_worker_proc, uargs) == 0) pthread_detach(tid);
                            else { t->is_running_now--; s->active_jobs--; FREE(uargs); }
                        } else {
                            JobNode* j = MALLOC(sizeof(JobNode)); 
                            j->func = t->func; j->arg = t->arg; j->task_id = t->id; j->next = NULL;
                            if (!s->job_tail) s->job_head = j; else s->job_tail->next = j;
                            s->job_tail = j;
                            s->queued_jobs++; // 큐에 작업 추가됨
                            
                            // ✅ [FIX 3] 노는 직원보다 대기표(큐)가 많으면 스레드 즉각 추가 생성!
                            int idle_workers = s->cur_workers - s->busy_workers;
                            if (s->queued_jobs > idle_workers && s->cur_workers < MAX_WORKERS) {
                                pthread_t tid; if (pthread_create(&tid, NULL, worker_proc, s) == 0) { pthread_detach(tid); s->cur_workers++; }
                            }
                            pthread_cond_signal(&s->job_cond);
                        }
                    } else {
						pthread_mutex_unlock(&s->lock);
						TaskContext ctx = { .sched = s, .user_arg = t->arg, .task_id = t->id }; 
						t->func(&ctx); 
						pthread_mutex_lock(&s->lock); 
						t->is_running_now--;
					}
                    if (t->is_periodic) {
						t->next_run = (t->type == TYPE_RELATIVE) ? timespec_add_ms(t->next_run, t->interval_ms) : get_next_calendar_monotonic(t->w, t->h, t->m);
					} else {
						t->is_active = 0;
					}
                }
            }

			if (t->is_active) {
				int is_waiting = (t->policy == POLICY_WAIT && t->is_running_now > 0);
				if (!is_waiting && (!has_t || timespec_cmp(&t->next_run, &next_w) < 0)) {
					next_w = t->next_run;
					has_t = 1;
				}
			}
		}
		if (!s->is_shutting_down) {
			if (has_t) {
				pthread_cond_timedwait(&s->wakeup_cond, &s->lock, &next_w);
			}
			else {
				pthread_cond_wait(&s->wakeup_cond, &s->lock);
			}
		}
		pthread_mutex_unlock(&s->lock);
	}
    return NULL;
}

/* ========================================================================= *
 * [5] 공용 API
 * ========================================================================= */
void scheduler_init(Scheduler* s)
{
	memset(s, 0, sizeof(Scheduler));
	s->capacity = INITIAL_CAPACITY;
	s->tasks = MALLOC(sizeof(Task) * s->capacity);
	s->next_task_id = 1;
	pthread_mutex_init(&s->lock, NULL);
	pthread_condattr_t attr;
	pthread_condattr_init(&attr);
	pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
	pthread_cond_init(&s->wakeup_cond, &attr);
	pthread_cond_init(&s->job_cond, &attr);
	pthread_condattr_destroy(&attr); 
}

void scheduler_start(Scheduler* s)
{
	pthread_mutex_lock(&s->lock);
	for (int i = 0; i < MIN_WORKERS; i++) {
		pthread_t tid;
		if (pthread_create(&tid, NULL, worker_proc, s) == 0) {
			pthread_detach(tid);
			s->cur_workers++;
		}
	}
	pthread_mutex_unlock(&s->lock);
	pthread_create(&s->scheduler_thread, NULL, scheduler_loop, s);
}

void scheduler_stop(Scheduler* s, int timeout_sec)
{
	pthread_mutex_lock(&s->lock);
	s->is_shutting_down = 1;
	pthread_cond_broadcast(&s->wakeup_cond);
	pthread_cond_broadcast(&s->job_cond);
	pthread_mutex_unlock(&s->lock);
	pthread_join(s->scheduler_thread, NULL);

	struct timespec ts = timespec_now_monotonic();
	ts.tv_sec += timeout_sec;
	pthread_mutex_lock(&s->lock);
	while (s->active_jobs > 0 && pthread_cond_timedwait(&s->wakeup_cond, &s->lock, &ts) != ETIMEDOUT);

	JobNode* curr = s->job_head;
	while (curr) {
		JobNode* next = curr->next;
		FREE(curr);
		curr = next;
	}
	s->job_head = NULL;
	s->job_tail = NULL;

	FREE(s->tasks);
	pthread_mutex_unlock(&s->lock);
	printf("\n[Memory Report] 최종 할당 카운트: %d (0이면 정상)\n", atomic_load(&internal_alloc_count));
	pthread_mutex_destroy(&s->lock);
	pthread_cond_destroy(&s->wakeup_cond);
	pthread_cond_destroy(&s->job_cond);
}

uint64_t scheduler_add_oneshot(Scheduler* s, long delay, int is_urgent, TaskFunc f, void* a)
{
	pthread_mutex_lock(&s->lock);

	int idx = get_available_slot(s);
	uint64_t id = s->next_task_id++;

	s->tasks[idx] = (Task){ .id = id, .is_active = 1, .is_periodic = 0,
		.type = TYPE_RELATIVE, .next_run = timespec_add_ms(timespec_now_monotonic(), delay),
		.interval_ms = 0, .w = 0, .h = 0, .m = 0,
		.use_thread = 1, .is_urgent = is_urgent, .policy = POLICY_OVERLAP,
		.is_running_now = 0, .func = f, .arg = a 
	};

	pthread_cond_signal(&s->wakeup_cond);
	pthread_mutex_unlock(&s->lock);
	return id;
}

uint64_t scheduler_add_periodic(Scheduler* s, long interval, int thr, int is_urgent, OverrunPolicy pol, TaskFunc f, void* a)
{
	pthread_mutex_lock(&s->lock);

	int idx = get_available_slot(s); 
	uint64_t id = s->next_task_id++;

	s->tasks[idx] = (Task){ .id = id, .is_active = 1, .is_periodic = 1, 
		.type = TYPE_RELATIVE, .next_run = timespec_add_ms(timespec_now_monotonic(), interval),
		.interval_ms = interval, .w = 0, .h = 0, .m = 0, .use_thread = thr, 
		.is_urgent = is_urgent, .policy = pol, .is_running_now = 0, 
		.func = f, .arg = a };

	pthread_cond_signal(&s->wakeup_cond); 
	pthread_mutex_unlock(&s->lock);
	return id;
}

uint64_t scheduler_add_calendar(Scheduler* s, int w, int h, int m, int thr, int is_urgent, OverrunPolicy pol, TaskFunc f, void* a)
{
	pthread_mutex_lock(&s->lock); 

	int idx = get_available_slot(s);
	uint64_t id = s->next_task_id++;

	s->tasks[idx] = (Task){ .id = id, .is_active = 1, .is_periodic = 1,
		.type = TYPE_CALENDAR, .next_run = get_next_calendar_monotonic(w, h, m),
		.interval_ms = 0, .w = w, .h = h, .m = m, .use_thread = thr, .is_urgent = is_urgent,
		.policy = pol, .is_running_now = 0,
		.func = f, .arg = a };

	pthread_cond_signal(&s->wakeup_cond); 
	pthread_mutex_unlock(&s->lock); 
	return id;
}

void scheduler_remove_task(Scheduler* s, uint64_t id)
{
	pthread_mutex_lock(&s->lock);

	Task* t = find_task(s, id);
	if (t) {
		t->is_active = 0;
	}

	pthread_mutex_unlock(&s->lock);
}

/* ========================================================================= *
 * [6] 테스트 코드 및 사용 예제 (Calendar & Verification)
 * ========================================================================= */

// 자동 검증을 위한 원자적 카운터
static atomic_int success_count = 0;

// ✅ [NEW] 날짜 및 시간을 깔끔한 문자열로 만들어주는 헬퍼 함수
void get_current_time_str(char* buf, size_t size) {
	time_t now = time(NULL);
	struct tm t;
	localtime_r(&now, &t); // ✅ Thread-Safe 함수 적용!
	const char* wday_name[] = {"일", "월", "화", "수", "목", "금", "토"};

	// 포인터(->) 대신 구조체 멤버(.) 접근으로 변경
	snprintf(buf, size, "%04d-%02d-%02d(%s) %02d:%02d:%02d",
			t.tm_year + 1900, t.tm_mon + 1, t.tm_mday,
			wday_name[t.tm_wday],
			t.tm_hour, t.tm_min, t.tm_sec);
}

// 캘린더 예제용 더미(Dummy) 작업 함수
void task_example_log(TaskContext* ctx) {
	char time_str[64];
    get_current_time_str(time_str, sizeof(time_str));

    // 로그 앞에 시간 문자열을 추가하여 출력
    printf("[%s] 📅 [캘린더 알림] %s (ID:%lu, Thread:%lu)\n",
           time_str, (char*)ctx->user_arg, ctx->task_id, pthread_self());
}

// 검증 테스트용 카운터 증가 함수
void task_verify_success(TaskContext* ctx) {
	char time_str[64];
	get_current_time_str(time_str, sizeof(time_str));

	atomic_fetch_add(&success_count, 1);
	printf("[%s] 🟢 [검증 작업] 실행 완료! (ID:%lu, 현재 카운트: %d)\n", 
			time_str, ctx->task_id, atomic_load(&success_count));
}

int main() {
    Scheduler s;
    scheduler_init(&s);
    scheduler_start(&s);

    printf("\n======================================================\n");
    printf("🚀 스케줄러 캘린더 예제 및 자동 검증 테스트 시작\n");
    printf("======================================================\n\n");

    /* ------------------------------------------------------------------
     * [Part 1] 실무 달력(Calendar) 스케줄링 사용 예제 모음
     * ------------------------------------------------------------------ */
    printf("📝 [Part 1] 실무에서 자주 쓰이는 캘린더 예약 예제 등록 중...\n");

    // ① [매시 특정분] 매시 정각(00분)에 실행 
    // 파라미터: 요일 -1(Any), 시간 -1(Any), 분 0
    scheduler_add_calendar(&s, DAY_ANY, TIME_ANY, 0, 1, 0, POLICY_OVERLAP, task_example_log, "매시 정각(00분) 데이터 동기화");

    // ② [매시 특정분] 매시 30분에 실행
    scheduler_add_calendar(&s, DAY_ANY, TIME_ANY, 30, 1, 0, POLICY_OVERLAP, task_example_log, "매시 30분 시스템 헬스 체크");

    // ③ [매일 특정시간] 매일 오후 2시 15분에 실행 
    // 파라미터: 요일 -1(Any), 시간 14(오후 2시), 분 15
    scheduler_add_calendar(&s, DAY_ANY, 14, 15, 1, 0, POLICY_OVERLAP, task_example_log, "매일 14:15 일일 정산 작업");

    // ④ [매주 특정요일/시간] 매주 금요일 퇴근 시간(오후 6시 00분)에 실행 
    // 파라미터: 요일 5(금요일), 시간 18(오후 6시), 분 0
    // ※ 요일 값: 0(일), 1(월), 2(화), 3(수), 4(목), 5(금), 6(토)
    scheduler_add_calendar(&s, DAY_FRI, 18, 0, 1, 0, POLICY_OVERLAP, task_example_log, "매주 금요일 18:00 주간 DB 백업");

    // ⑤ [매주 다중요일/시간] 매주 월, 수, 금 오전 9시에 실행
    // ※ 동일한 작업을 요일만 바꿔서 3번 등록하는 패턴 사용
    TaskFunc mwf_task = task_example_log;
    scheduler_add_calendar(&s, DAY_MON, 9, 0, 1, 0, POLICY_OVERLAP, mwf_task, "월/수/금 09:00 시스템 점검 (월)");
    scheduler_add_calendar(&s, DAY_WED, 9, 0, 1, 0, POLICY_OVERLAP, mwf_task, "월/수/금 09:00 시스템 점검 (수)");
    scheduler_add_calendar(&s, DAY_FRI, 9, 0, 1, 0, POLICY_OVERLAP, mwf_task, "월/수/금 09:00 시스템 점검 (금)");

    scheduler_add_periodic(&s, 5*60*1000, 1, 0, POLICY_OVERLAP, task_verify_success, NULL);

    printf("  -> ✅ 다양한 캘린더 예약이 큐에 안전하게 등록되었습니다. (실제 지정 시간에 도달하면 실행됨)\n\n");

	sleep(60*60*24*10);

    /* ------------------------------------------------------------------
     * [Part 2] 스케줄러 코어 엔진 정상 동작 자동 검증 테스트
     * ------------------------------------------------------------------ */
    printf("📝 [Part 2] 스케줄러 자동 검증 테스트 시작\n");
    atomic_store(&success_count, 0);

    // 1. VIP(Urgent) 긴급 작업: 0ms 대기, 즉시 실행 (예상 카운트 +1)
    scheduler_add_oneshot(&s, 0, 1, task_verify_success, NULL);

    // 2. 일반 1회성 작업: 100ms 뒤 실행 (예상 카운트 +1)
    scheduler_add_oneshot(&s, 100, 0, task_verify_success, NULL);

    // 3. 일반 1회성 작업: 300ms 뒤 실행 (예상 카운트 +1)
    scheduler_add_oneshot(&s, 300, 0, task_verify_success, NULL);

    // 4. 주기적 작업: 500ms 주기 (1초 대기할 것이므로 500ms쯤 1번만 실행됨) (예상 카운트 +1)
    scheduler_add_periodic(&s, 500, 1, 0, POLICY_OVERLAP, task_verify_success, NULL);

    // 5. 작업 취소 테스트: 200ms 뒤 실행 예정이지만 즉시 삭제 (예상 카운트 변동 없어야 함)
    uint64_t c_id = scheduler_add_oneshot(&s, 200, 0, task_verify_success, NULL);
    scheduler_remove_task(&s, c_id); 

    printf("\n⏳ 검증 작업들이 완료될 때까지 1초간 대기합니다...\n\n");
    sleep(1);

    printf("\n🛑 스케줄러 종료 시퀀스 시작...\n");
    // timeout 5초 지정, 이 과정에서 메모리와 남은 큐가 모두 정리됩니다.
    scheduler_stop(&s, 5); 

    /* ------------------------------------------------------------------
     * [Part 3] 결과 자동 판별 (Assertion)
     * ------------------------------------------------------------------ */
    printf("\n======================================================\n");
    printf("📊 테스트 결과 검증 리포트\n");
    printf("======================================================\n");
    
    int final_count = atomic_load(&success_count);
    int mem_leaks = atomic_load(&internal_alloc_count);
    int is_success = 1;

    // 카운트 검증: 총 4번(VIP 1번 + Oneshot 2번 + Periodic 1번) 실행되어야 정상
    if (final_count == 4) {
        printf("✅ 작업 실행 타이밍 및 취소 로직 검증 : [ PASS ] (예상: 4, 실제: %d)\n", final_count);
    } else {
        printf("❌ 작업 실행 타이밍 및 취소 로직 검증 : [ FAIL ] (예상: 4, 실제: %d)\n", final_count);
        is_success = 0;
    }

    // 메모리 누수 검증
    if (mem_leaks == 0) {
        printf("✅ 동적 할당 메모리 누수(Leak) 검증   : [ PASS ] (Leaked Blocks: 0)\n");
    } else {
        printf("❌ 동적 할당 메모리 누수(Leak) 검증   : [ FAIL ] (Leaked Blocks: %d)\n", mem_leaks);
        is_success = 0;
    }

    // 최종 결과 출력
    if (is_success) {
        printf("\n🎉 [ALL TESTS PASSED] 축하합니다! 스케줄러가 완벽하게 동작합니다!\n\n");
    } else {
        printf("\n⚠️ [TEST FAILED] 버그가 발견되었습니다. 타이밍이나 메모리 해제 로직을 점검해 주세요.\n\n");
    }

    return 0;
}
