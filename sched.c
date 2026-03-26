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
 * [1] 메모리 트래킹 엔진 (Memory Leak 방지)
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
#define TIME_ANY -1

typedef enum { POLICY_OVERLAP = 0, POLICY_SKIP = 1, POLICY_WAIT = 2 } OverrunPolicy;
typedef enum { TYPE_RELATIVE = 0, TYPE_CALENDAR = 1 } ScheduleType;

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

typedef struct Scheduler Scheduler;
typedef struct TaskContext TaskContext;
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
    
    struct timespec next_run; // 🟢 상대 시간(TYPE_RELATIVE) 전용 (Monotonic)
    time_t target_realtime;   // 🔴 캘린더(TYPE_CALENDAR) 전용 (Realtime)
    
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
    int queued_jobs; 
};

/* ========================================================================= *
 * [3] 유틸리티 및 시간 계산 (하이브리드 & Thread-Safe 적용)
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

// 캘린더 시간 산출 (시스템 변경 시간 즉각 반영 & Thread-Safe)
time_t get_next_calendar_realtime(int tw, int th, int tm_min) {
    time_t now_t = time(NULL);
    struct tm t;
    localtime_r(&now_t, &t); // ✅ Thread-Safe 함수 적용
    t.tm_sec = 0; 

    if (tm_min >= 0) t.tm_min = tm_min;
    if (th >= 0) t.tm_hour = th;
    
    time_t next_t = mktime(&t);

    if (next_t <= now_t) {
        if (th < 0) t.tm_hour++; 
        else if (tw < 0) t.tm_mday++; 
        else t.tm_mday += 7; 
        next_t = mktime(&t); 
    }

    if (tw >= 0) {
        if (t.tm_wday != tw) {
            int diff = (tw - t.tm_wday + 7) % 7;
            t.tm_mday += diff; 
            if (th < 0) t.tm_hour = 0; // 엣지 케이스 방어
            next_t = mktime(&t);
        }
    }
    return next_t; 
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
typedef struct { Scheduler* sched; TaskFunc func; void* arg; uint64_t task_id; } UrgentArgs;

void* urgent_worker_proc(void* arg) {
    UrgentArgs* uargs = (UrgentArgs*)arg;
    Scheduler* s = uargs->sched; uint64_t task_id = uargs->task_id;
    
    TaskContext ctx = { .sched = s, .user_arg = uargs->arg, .task_id = task_id };
    uargs->func(&ctx);

    pthread_mutex_lock(&s->lock); s->active_jobs--; 
    Task* t = find_task(s, task_id); if (t) t->is_running_now--;
    if ((t && t->is_running_now == 0) || s->active_jobs == 0) pthread_cond_broadcast(&s->wakeup_cond);
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
                    s->cur_workers--; pthread_mutex_unlock(&s->lock); return NULL; 
                }
            }
        }
        
        if (s->is_shutting_down) { s->cur_workers--; pthread_mutex_unlock(&s->lock); break; }
        
        JobNode* job = s->job_head; s->job_head = job->next;
        if (!s->job_head) s->job_tail = NULL;

        s->queued_jobs--; s->busy_workers++; s->active_jobs++;
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
        
        struct timespec now_mono = timespec_now_monotonic(); 
        time_t now_real = time(NULL);                        
        
        // 🚨 최대 1초(1000ms)씩 끊어서 대기 (하이브리드 폴링)
        struct timespec next_w = timespec_add_ms(now_mono, 1000); 

        for (int i = 0; i < s->task_count; i++) {
            Task* t = &s->tasks[i];
            if (!t->is_active) continue;
            
            int run = 0;
            if (t->type == TYPE_RELATIVE) {
                if (timespec_cmp(&t->next_run, &now_mono) <= 0) run = 1;
            } else {
                if (t->target_realtime <= now_real) run = 1;
            }

            if (run) {
                int actual_run = 1;
                if (t->is_running_now > 0) {
                    if (t->policy == POLICY_SKIP) {
                        actual_run = 0;
                        if (t->type == TYPE_RELATIVE) t->next_run = timespec_add_ms(t->next_run, t->interval_ms);
                        else t->target_realtime = get_next_calendar_realtime(t->w, t->h, t->m);
                    } else if (t->policy == POLICY_WAIT) {
                        actual_run = 0;
                    }
                }
                
                if (actual_run) {
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
                            s->queued_jobs++;
                            
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
                        if (t->type == TYPE_RELATIVE) t->next_run = timespec_add_ms(t->next_run, t->interval_ms);
                        else t->target_realtime = get_next_calendar_realtime(t->w, t->h, t->m);
                    } else {
                        t->is_active = 0;
                    }
                }
            }

            if (t->is_active) {
                int is_waiting = (t->policy == POLICY_WAIT && t->is_running_now > 0);
                if (!is_waiting) {
                    // 상대시간 작업에 한해서만 짧은 알람으로 갱신 (캘린더는 무조건 1초마다 깸)
                    if (t->type == TYPE_RELATIVE && timespec_cmp(&t->next_run, &next_w) < 0) {
                        next_w = t->next_run;
                    }
                }
            }
        }
        
        if (!s->is_shutting_down) {
            pthread_cond_timedwait(&s->wakeup_cond, &s->lock, &next_w);
        }
        pthread_mutex_unlock(&s->lock);
    }
    return NULL;
}

/* ========================================================================= *
 * [5] 공용 API
 * ========================================================================= */
void scheduler_init(Scheduler* s) {
    memset(s, 0, sizeof(Scheduler));
    s->capacity = INITIAL_CAPACITY;
    s->tasks = MALLOC(sizeof(Task) * s->capacity);
    s->next_task_id = 1;
    pthread_mutex_init(&s->lock, NULL);
    pthread_condattr_t attr; pthread_condattr_init(&attr); pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
    pthread_cond_init(&s->wakeup_cond, &attr); pthread_cond_init(&s->job_cond, &attr);
    pthread_condattr_destroy(&attr); 
}

void scheduler_start(Scheduler* s) {
    pthread_mutex_lock(&s->lock);
    for (int i = 0; i < MIN_WORKERS; i++) {
        pthread_t tid;
        if (pthread_create(&tid, NULL, worker_proc, s) == 0) {
            pthread_detach(tid); s->cur_workers++;
        }
    }
    pthread_mutex_unlock(&s->lock);
    pthread_create(&s->scheduler_thread, NULL, scheduler_loop, s);
}

void scheduler_stop(Scheduler* s, int timeout_sec) {
    pthread_mutex_lock(&s->lock);
    s->is_shutting_down = 1;
    pthread_cond_broadcast(&s->wakeup_cond); pthread_cond_broadcast(&s->job_cond);
    pthread_mutex_unlock(&s->lock);
    pthread_join(s->scheduler_thread, NULL);

    struct timespec ts = timespec_now_monotonic();
    ts.tv_sec += timeout_sec;
    
    pthread_mutex_lock(&s->lock);
    while (s->active_jobs > 0 && pthread_cond_timedwait(&s->wakeup_cond, &s->lock, &ts) != ETIMEDOUT);

    JobNode* curr = s->job_head;
    while (curr) { JobNode* next = curr->next; FREE(curr); curr = next; }
    s->job_head = NULL; s->job_tail = NULL;

    FREE(s->tasks);
    pthread_mutex_unlock(&s->lock);
    printf("\n[Memory Report] 최종 할당 카운트: %d (0이면 정상)\n", atomic_load(&internal_alloc_count));
    
    pthread_mutex_destroy(&s->lock);
    pthread_cond_destroy(&s->wakeup_cond); pthread_cond_destroy(&s->job_cond);
}

uint64_t scheduler_add_oneshot(Scheduler* s, long delay, int is_urgent, TaskFunc f, void* a) {
    pthread_mutex_lock(&s->lock);
    int idx = get_available_slot(s); uint64_t id = s->next_task_id++;

    s->tasks[idx] = (Task){ .id = id, .is_active = 1, .is_periodic = 0,
        .type = TYPE_RELATIVE, .next_run = timespec_add_ms(timespec_now_monotonic(), delay),
        .interval_ms = 0, .w = 0, .h = 0, .m = 0,
        .use_thread = 1, .is_urgent = is_urgent, .policy = POLICY_OVERLAP,
        .is_running_now = 0, .func = f, .arg = a 
    };

    pthread_cond_signal(&s->wakeup_cond); pthread_mutex_unlock(&s->lock);
    return id;
}

uint64_t scheduler_add_periodic(Scheduler* s, long interval, int thr, int is_urgent, OverrunPolicy pol, TaskFunc f, void* a) {
    pthread_mutex_lock(&s->lock);
    int idx = get_available_slot(s); uint64_t id = s->next_task_id++;

    s->tasks[idx] = (Task){ .id = id, .is_active = 1, .is_periodic = 1, 
        .type = TYPE_RELATIVE, .next_run = timespec_add_ms(timespec_now_monotonic(), interval),
        .interval_ms = interval, .w = 0, .h = 0, .m = 0, .use_thread = thr, 
        .is_urgent = is_urgent, .policy = pol, .is_running_now = 0, 
        .func = f, .arg = a };

    pthread_cond_signal(&s->wakeup_cond); pthread_mutex_unlock(&s->lock);
    return id;
}

uint64_t scheduler_add_calendar(Scheduler* s, int w, int h, int m, int thr, int is_urgent, OverrunPolicy pol, TaskFunc f, void* a) {
    pthread_mutex_lock(&s->lock); 
    int idx = get_available_slot(s); uint64_t id = s->next_task_id++;

    s->tasks[idx] = (Task){ .id = id, .is_active = 1, .is_periodic = 1,
        .type = TYPE_CALENDAR, .target_realtime = get_next_calendar_realtime(w, h, m),
        .interval_ms = 0, .w = w, .h = h, .m = m, .use_thread = thr, .is_urgent = is_urgent,
        .policy = pol, .is_running_now = 0, .func = f, .arg = a };

    pthread_cond_signal(&s->wakeup_cond); pthread_mutex_unlock(&s->lock); 
    return id;
}

void scheduler_remove_task(Scheduler* s, uint64_t id) {
    pthread_mutex_lock(&s->lock);
    Task* t = find_task(s, id); if (t) t->is_active = 0;
    pthread_mutex_unlock(&s->lock);
}

/* ========================================================================= *
 * [6] 테스트 코드 및 사용 예제 (Calendar & Verification)
 * ========================================================================= */

static atomic_int success_count = 0;

void get_current_time_str(char* buf, size_t size) {
    time_t now = time(NULL);
    struct tm t;
    localtime_r(&now, &t); // ✅ Thread-Safe 함수
    const char* wday_name[] = {"일", "월", "화", "수", "목", "금", "토"};
    snprintf(buf, size, "%04d-%02d-%02d(%s) %02d:%02d:%02d",
             t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, wday_name[t.tm_wday], t.tm_hour, t.tm_min, t.tm_sec);
}

void task_example_log(TaskContext* ctx) {
    char time_str[64]; get_current_time_str(time_str, sizeof(time_str));
    printf("[%s] 📅 [캘린더 알림] %s (ID:%lu, Thread:%lu)\n", time_str, (char*)ctx->user_arg, ctx->task_id, pthread_self());
}

void task_verify_success(TaskContext* ctx) {
    char time_str[64]; get_current_time_str(time_str, sizeof(time_str));
    atomic_fetch_add(&success_count, 1);
    printf("[%s] 🟢 [검증 작업] 실행 완료! (ID:%lu, 현재 카운트: %d, Thread:%lu)\n", time_str, ctx->task_id, atomic_load(&success_count), pthread_self());
}

int main() {
    Scheduler s;
    scheduler_init(&s);
    scheduler_start(&s);

    printf("\n======================================================\n");
    printf("🚀 스케줄러 캘린더 예제 및 자동 검증 테스트 시작\n");
    printf("======================================================\n\n");

    printf("📝 [Part 1] 실무 달력(Calendar) 예약 예제 등록 중...\n");

    scheduler_add_calendar(&s, DAY_ANY, TIME_ANY, 0, 1, 0, POLICY_OVERLAP, task_example_log, "매시 정각(00분) 데이터 동기화");
    scheduler_add_calendar(&s, DAY_ANY, TIME_ANY, 30, 1, 0, POLICY_OVERLAP, task_example_log, "매시 30분 시스템 헬스 체크");
    scheduler_add_calendar(&s, DAY_ANY, 14, 15, 1, 0, POLICY_OVERLAP, task_example_log, "매일 14:15 일일 정산 작업");
    scheduler_add_calendar(&s, DAY_FRI, 18, 0, 1, 0, POLICY_OVERLAP, task_example_log, "매주 금요일 18:00 주간 DB 백업");

    TaskFunc mwf_task = task_example_log;
    scheduler_add_calendar(&s, DAY_MON, 9, 0, 1, 0, POLICY_OVERLAP, mwf_task, "월/수/금 09:00 시스템 점검 (월)");
    scheduler_add_calendar(&s, DAY_WED, 9, 0, 1, 0, POLICY_OVERLAP, mwf_task, "월/수/금 09:00 시스템 점검 (수)");
    scheduler_add_calendar(&s, DAY_FRI, 9, 0, 1, 0, POLICY_OVERLAP, mwf_task, "월/수/금 09:00 시스템 점검 (금)");

    // 테스트를 위한 주기적 핑 작업
    scheduler_add_periodic(&s, 30*60*1000, 1, 0, POLICY_OVERLAP, task_example_log, "⏱️ 5초 주기 핑(Ping) 테스트");

    scheduler_add_oneshot(&s, 0, 1, task_verify_success, NULL);
    scheduler_add_oneshot(&s, 1000, 0, task_verify_success, NULL);
    scheduler_add_oneshot(&s, 3000, 0, task_verify_success, NULL);

    printf("  -> ✅ 다양한 캘린더 예약이 큐에 안전하게 등록되었습니다.\n\n");

	sleep(60*60*64);

    printf("📝 [Part 2] 스케줄러 코어 엔진 자동 검증 시작\n");
    atomic_store(&success_count, 0);

    scheduler_add_oneshot(&s, 0, 1, task_verify_success, NULL);
    scheduler_add_oneshot(&s, 100, 0, task_verify_success, NULL);
    scheduler_add_oneshot(&s, 300, 0, task_verify_success, NULL);
    scheduler_add_periodic(&s, 500, 1, 0, POLICY_OVERLAP, task_verify_success, NULL);

    uint64_t c_id = scheduler_add_oneshot(&s, 200, 0, task_verify_success, NULL);
    scheduler_remove_task(&s, c_id); 

    printf("\n⏳ 검증 작업들이 완료될 때까지 1초간 대기합니다...\n\n");
    sleep(1);

    // 🚨 캘린더 작업과 핑을 오래 지켜보고 싶거나, 터미널에서 date 로 시간을 변경하며 테스트하고 싶다면 주석을 푸세요!
    // printf("\n⏳ 캘린더 작업을 확인하기 위해 스케줄러를 계속 켜둡니다 (강제 종료는 Ctrl+C)\n\n");
    // sleep(3600); 

    printf("\n🛑 스케줄러 종료 시퀀스 시작...\n");
    scheduler_stop(&s, 5); 

    printf("\n======================================================\n");
    printf("📊 테스트 결과 검증 리포트\n");
    printf("======================================================\n");
    
    int final_count = atomic_load(&success_count);
    int mem_leaks = atomic_load(&internal_alloc_count);
    int is_success = 1;

    if (final_count == 4) {
        printf("✅ 작업 실행 타이밍 및 취소 로직 검증 : [ PASS ] (예상: 4, 실제: %d)\n", final_count);
    } else {
        printf("❌ 작업 실행 타이밍 및 취소 로직 검증 : [ FAIL ] (예상: 4, 실제: %d)\n", final_count);
        is_success = 0;
    }

    if (mem_leaks == 0) {
        printf("✅ 동적 할당 메모리 누수(Leak) 검증   : [ PASS ] (Leaked Blocks: 0)\n");
    } else {
        printf("❌ 동적 할당 메모리 누수(Leak) 검증   : [ FAIL ] (Leaked Blocks: %d)\n", mem_leaks);
        is_success = 0;
    }

    if (is_success) printf("\n🎉 [ALL TESTS PASSED] 축하합니다! 스케줄러가 완벽하게 동작합니다!\n\n");
    else printf("\n⚠️ [TEST FAILED] 버그가 발견되었습니다.\n\n");

    return 0;
}
