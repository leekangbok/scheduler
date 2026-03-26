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

void *tracker_malloc(size_t size)
{
	if (size == 0) {
		return NULL;
	}

	atomic_fetch_add(&internal_alloc_count, 1);
	return malloc(size);
}

void tracker_free(void *ptr)
{
	if (!ptr) {
		return;
	}

	free(ptr);
	atomic_fetch_sub(&internal_alloc_count, 1);
}

void *tracker_realloc(void *ptr, size_t size)
{
	if (!ptr) {
		return tracker_malloc(size);
	}

	if (size == 0) {
		tracker_free(ptr);
		return NULL;
	}

	return realloc(ptr, size);
}

#define MALLOC tracker_malloc
#define REALLOC tracker_realloc
#define FREE tracker_free

/* ========================================================================= *
 * [2] 상수 및 자료구조
 * ========================================================================= */
#define MIN_WORKERS             4
#define MAX_WORKERS             20
#define IDLE_TIMEOUT_SEC        5
#define INITIAL_CAPACITY        16
#define TIME_ANY                -1

enum overrun_policy {
	POLICY_OVERLAP = 0,
	POLICY_SKIP = 1,
	POLICY_WAIT = 2
};

enum schedule_type {
	TYPE_RELATIVE = 0,
	TYPE_CALENDAR = 1
};

enum week_day {
	DAY_ANY = -1,
	DAY_SUN = 0,
	DAY_MON = 1,
	DAY_TUE = 2,
	DAY_WED = 3,
	DAY_THU = 4,
	DAY_FRI = 5,
	DAY_SAT = 6
};

struct scheduler;
struct task_context;

typedef void (*task_func_t)(struct task_context *ctx);

struct task_context {
	struct scheduler *sched;
	void *user_arg;
	uint64_t task_id;
};

struct job_node {
	task_func_t func;
	void *arg;
	uint64_t task_id;
	struct job_node *next;
};

struct task {
	uint64_t id;
	int is_active;
	int is_periodic;
	enum schedule_type type;

	struct timespec next_run;
	time_t target_realtime;

	long interval_ms;
	int w;
	int h;
	int m;
	int use_thread;
	int is_urgent;
	enum overrun_policy policy;
	int is_running_now;
	task_func_t func;
	void *arg;
};

struct scheduler {
	struct task *tasks;
	int capacity;
	int task_count;
	uint64_t next_task_id;
	pthread_mutex_t lock;
	volatile int is_shutting_down;
	pthread_t scheduler_thread;
	pthread_cond_t wakeup_cond;
	struct job_node *job_head;
	struct job_node *job_tail;
	pthread_cond_t job_cond;
	int cur_workers;
	int busy_workers;
	int active_jobs;
	int queued_jobs;
};

struct urgent_args {
	struct scheduler *sched;
	task_func_t func;
	void *arg;
	uint64_t task_id;
};

/* ========================================================================= *
 * [3] 유틸리티 및 시간 계산
 * ========================================================================= */
struct timespec timespec_now_monotonic(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts;
}

struct timespec timespec_add_ms(struct timespec ts, long ms)
{
	ts.tv_sec += ms / 1000;
	ts.tv_nsec += (ms % 1000) * 1000000L;

	if (ts.tv_nsec >= 1000000000L) {
		ts.tv_sec++;
		ts.tv_nsec -= 1000000000L;
	}

	return ts;
}

int timespec_cmp(struct timespec *a, struct timespec *b)
{
	if (a->tv_sec != b->tv_sec) {
		return a->tv_sec < b->tv_sec ? -1 : 1;
	}

	if (a->tv_nsec < b->tv_nsec) {
		return -1;
	} else if (a->tv_nsec > b->tv_nsec) {
		return 1;
	}

	return 0;
}

time_t get_next_calendar_realtime(int tw, int th, int tm_min)
{
	time_t now_t = time(NULL);
	struct tm t;
	time_t next_t;
	int diff;

	localtime_r(&now_t, &t);
	t.tm_sec = 0;

	if (tm_min >= 0) {
		t.tm_min = tm_min;
	}
	if (th >= 0) {
		t.tm_hour = th;
	}

	next_t = mktime(&t);

	if (next_t <= now_t) {
		if (th < 0) {
			t.tm_hour++;
		} else if (tw < 0) {
			t.tm_mday++;
		} else {
			t.tm_mday += 7;
		}

		t.tm_isdst = -1; /* 서머타임(DST) 시스템 재판단 지시 */
		next_t = mktime(&t);
	}

	if (tw >= 0) {
		if (t.tm_wday != tw) {
			diff = (tw - t.tm_wday + 7) % 7;
			t.tm_mday += diff;

			if (th < 0) {
				t.tm_hour = 0;
			}

			t.tm_isdst = -1; /* 요일 점프 시에도 DST 재판단 */
			next_t = mktime(&t);
		}
	}

	return next_t;
}

/* ========================================================================= *
 * [4] 슬롯 관리 및 코어 로직
 * ========================================================================= */
static int get_available_slot(struct scheduler *s)
{
	int i;

	for (i = 0; i < s->task_count; i++) {
		if (!s->tasks[i].is_active && s->tasks[i].is_running_now == 0) {
			return i;
		}
	}

	if (s->task_count >= s->capacity) {
		s->capacity *= 2;
		s->tasks = REALLOC(s->tasks, sizeof(struct task) * s->capacity);
	}

	return s->task_count++;
}

struct task *find_task(struct scheduler *s, uint64_t id)
{
	int i;

	for (i = 0; i < s->task_count; i++) {
		if (s->tasks[i].id == id) {
			return &s->tasks[i];
		}
	}

	return NULL;
}

void *urgent_worker_proc(void *arg)
{
	struct urgent_args *uargs = arg;
	struct scheduler *s = uargs->sched;
	uint64_t task_id = uargs->task_id;
	struct task *t;
	struct task_context ctx = {
		.sched = s,
		.user_arg = uargs->arg,
		.task_id = task_id
	};

	uargs->func(&ctx);

	pthread_mutex_lock(&s->lock);
	s->active_jobs--;

	t = find_task(s, task_id);
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

void *worker_proc(void *arg)
{
	struct scheduler *s = arg;
	struct job_node *job;
	struct task *t;
	struct timespec out;
	struct task_context ctx;

	while (1) {
		pthread_mutex_lock(&s->lock);

		while (s->job_head == NULL && !s->is_shutting_down) {
			out = timespec_now_monotonic();
			out.tv_sec += IDLE_TIMEOUT_SEC;

			if (pthread_cond_timedwait(&s->job_cond, &s->lock, &out) == ETIMEDOUT) {
				if (s->cur_workers > MIN_WORKERS) {
					s->cur_workers--;
					pthread_mutex_unlock(&s->lock);
					return NULL;
				}
			}
		}

		if (s->is_shutting_down) {
			s->cur_workers--;
			pthread_mutex_unlock(&s->lock);
			break;
		}

		job = s->job_head;
		s->job_head = job->next;
		if (!s->job_head) {
			s->job_tail = NULL;
		}

		s->queued_jobs--;
		s->busy_workers++;
		s->active_jobs++;
		pthread_mutex_unlock(&s->lock);

		ctx.sched = s;
		ctx.user_arg = job->arg;
		ctx.task_id = job->task_id;
		job->func(&ctx);

		pthread_mutex_lock(&s->lock);
		s->busy_workers--;
		s->active_jobs--;

		t = find_task(s, job->task_id);
		if (t) {
			t->is_running_now--;
		}

		FREE(job);

		if ((t && t->is_running_now == 0) || s->active_jobs == 0) {
			pthread_cond_broadcast(&s->wakeup_cond);
		}

		pthread_mutex_unlock(&s->lock);
	}

	return NULL;
}

void *scheduler_loop(void *arg)
{
	struct scheduler *s = arg;
	struct timespec now_mono, next_w;
	time_t now_real;
	struct task *t;
	struct urgent_args *uargs;
	struct job_node *j;
	struct task_context ctx;
	pthread_t tid;
	int i, run, actual_run, idle_workers, is_waiting;

	while (!s->is_shutting_down) {
		pthread_mutex_lock(&s->lock);

		now_mono = timespec_now_monotonic();
		now_real = time(NULL);
		next_w = timespec_add_ms(now_mono, 1000);

		for (i = 0; i < s->task_count; i++) {
			t = &s->tasks[i];

			if (!t->is_active) {
				continue;
			}

			run = 0;
			if (t->type == TYPE_RELATIVE) {
				if (timespec_cmp(&t->next_run, &now_mono) <= 0) {
					run = 1;
				}
			} else {
				if (t->target_realtime <= now_real) {
					run = 1;
				}
			}

			if (run) {
				actual_run = 1;
				if (t->is_running_now > 0) {
					if (t->policy == POLICY_SKIP) {
						actual_run = 0;
						if (t->type == TYPE_RELATIVE) {
							t->next_run = timespec_add_ms(t->next_run, t->interval_ms);
						} else {
							t->target_realtime = get_next_calendar_realtime(t->w, t->h, t->m);
						}
					} else if (t->policy == POLICY_WAIT) {
						actual_run = 0;
					}
				}

				if (actual_run) {
					t->is_running_now++;
					if (t->use_thread) {
						if (t->is_urgent) {
							s->active_jobs++;
							uargs = MALLOC(sizeof(struct urgent_args));
							uargs->sched = s;
							uargs->func = t->func;
							uargs->arg = t->arg;
							uargs->task_id = t->id;

							if (pthread_create(&tid, NULL, urgent_worker_proc, uargs) == 0) {
								pthread_detach(tid);
							} else {
								t->is_running_now--;
								s->active_jobs--;
								FREE(uargs);
							}
						} else {
							j = MALLOC(sizeof(struct job_node));
							j->func = t->func;
							j->arg = t->arg;
							j->task_id = t->id;
							j->next = NULL;

							if (!s->job_tail) {
								s->job_head = j;
							} else {
								s->job_tail->next = j;
							}
							s->job_tail = j;
							s->queued_jobs++;

							idle_workers = s->cur_workers - s->busy_workers;
							if (s->queued_jobs > idle_workers && s->cur_workers < MAX_WORKERS) {
								if (pthread_create(&tid, NULL, worker_proc, s) == 0) {
									pthread_detach(tid);
									s->cur_workers++;
								}
							}
							pthread_cond_signal(&s->job_cond);
						}
					} else {
						pthread_mutex_unlock(&s->lock);
						ctx.sched = s;
						ctx.user_arg = t->arg;
						ctx.task_id = t->id;
						t->func(&ctx);
						pthread_mutex_lock(&s->lock);

						/* 락 해제 후 배열이 REALLOC 되었을 수 있으므로 포인터 주소 재할당 */
						t = &s->tasks[i];
						t->is_running_now--;
					}

					if (t->is_periodic) {
						if (t->type == TYPE_RELATIVE) {
							t->next_run = timespec_add_ms(t->next_run, t->interval_ms);
						} else {
							t->target_realtime = get_next_calendar_realtime(t->w, t->h, t->m);
						}
					} else {
						t->is_active = 0;
					}
				}
			}

			if (t->is_active) {
				is_waiting = (t->policy == POLICY_WAIT && t->is_running_now > 0);
				if (!is_waiting) {
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
void scheduler_init(struct scheduler *s)
{
	pthread_condattr_t attr;

	memset(s, 0, sizeof(struct scheduler));
	s->capacity = INITIAL_CAPACITY;
	s->tasks = MALLOC(sizeof(struct task) * s->capacity);
	s->next_task_id = 1;

	pthread_mutex_init(&s->lock, NULL);
	pthread_condattr_init(&attr);
	pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
	pthread_cond_init(&s->wakeup_cond, &attr);
	pthread_cond_init(&s->job_cond, &attr);
	pthread_condattr_destroy(&attr);
}

void scheduler_start(struct scheduler *s)
{
	pthread_t tid;
	int i;

	pthread_mutex_lock(&s->lock);
	for (i = 0; i < MIN_WORKERS; i++) {
		if (pthread_create(&tid, NULL, worker_proc, s) == 0) {
			pthread_detach(tid);
			s->cur_workers++;
		}
	}
	pthread_mutex_unlock(&s->lock);

	pthread_create(&s->scheduler_thread, NULL, scheduler_loop, s);
}

void scheduler_stop(struct scheduler *s, int timeout_sec)
{
	struct timespec ts;
	struct job_node *curr, *next;

	pthread_mutex_lock(&s->lock);
	s->is_shutting_down = 1;
	pthread_cond_broadcast(&s->wakeup_cond);
	pthread_cond_broadcast(&s->job_cond);
	pthread_mutex_unlock(&s->lock);

	pthread_join(s->scheduler_thread, NULL);

	ts = timespec_now_monotonic();
	ts.tv_sec += timeout_sec;

	pthread_mutex_lock(&s->lock);
	while (s->active_jobs > 0 && pthread_cond_timedwait(&s->wakeup_cond, &s->lock, &ts) != ETIMEDOUT) {
		/* Do nothing */
	}

	/* 타임아웃이 넘었는데도 일하는 좀비 스레드가 있다면, 메모리 해제를 포기하고 크래시를 방지함 */
	if (s->active_jobs > 0) {
		printf("\n[Warning] 타임아웃 초과! 워커 스레드가 아직 실행 중이므로 메모리 강제 해제를 스킵합니다.\n");
		pthread_mutex_unlock(&s->lock);
		return;
	}

	curr = s->job_head;
	while (curr) {
		next = curr->next;
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

uint64_t scheduler_add_oneshot(struct scheduler *s, long delay, int is_urgent, task_func_t f, void *a)
{
	int idx;
	uint64_t id;

	pthread_mutex_lock(&s->lock);
	idx = get_available_slot(s);
	id = s->next_task_id++;

	s->tasks[idx].id = id;
	s->tasks[idx].is_active = 1;
	s->tasks[idx].is_periodic = 0;
	s->tasks[idx].type = TYPE_RELATIVE;
	s->tasks[idx].next_run = timespec_add_ms(timespec_now_monotonic(), delay);
	s->tasks[idx].interval_ms = 0;
	s->tasks[idx].w = 0;
	s->tasks[idx].h = 0;
	s->tasks[idx].m = 0;
	s->tasks[idx].use_thread = 1;
	s->tasks[idx].is_urgent = is_urgent;
	s->tasks[idx].policy = POLICY_OVERLAP;
	s->tasks[idx].is_running_now = 0;
	s->tasks[idx].func = f;
	s->tasks[idx].arg = a;

	pthread_cond_signal(&s->wakeup_cond);
	pthread_mutex_unlock(&s->lock);

	return id;
}

uint64_t scheduler_add_periodic(struct scheduler *s, long interval, int thr, int is_urgent,
		enum overrun_policy pol, task_func_t f, void *a)
{
	int idx;
	uint64_t id;

	pthread_mutex_lock(&s->lock);
	idx = get_available_slot(s);
	id = s->next_task_id++;

	s->tasks[idx].id = id;
	s->tasks[idx].is_active = 1;
	s->tasks[idx].is_periodic = 1;
	s->tasks[idx].type = TYPE_RELATIVE;
	s->tasks[idx].next_run = timespec_add_ms(timespec_now_monotonic(), interval);
	s->tasks[idx].interval_ms = interval;
	s->tasks[idx].w = 0;
	s->tasks[idx].h = 0;
	s->tasks[idx].m = 0;
	s->tasks[idx].use_thread = thr;
	s->tasks[idx].is_urgent = is_urgent;
	s->tasks[idx].policy = pol;
	s->tasks[idx].is_running_now = 0;
	s->tasks[idx].func = f;
	s->tasks[idx].arg = a;

	pthread_cond_signal(&s->wakeup_cond);
	pthread_mutex_unlock(&s->lock);

	return id;
}

uint64_t scheduler_add_calendar(struct scheduler *s, int w, int h, int m, int thr, int is_urgent,
		enum overrun_policy pol, task_func_t f, void *a)
{
	int idx;
	uint64_t id;

	pthread_mutex_lock(&s->lock);
	idx = get_available_slot(s);
	id = s->next_task_id++;

	s->tasks[idx].id = id;
	s->tasks[idx].is_active = 1;
	s->tasks[idx].is_periodic = 1;
	s->tasks[idx].type = TYPE_CALENDAR;
	s->tasks[idx].target_realtime = get_next_calendar_realtime(w, h, m);
	s->tasks[idx].interval_ms = 0;
	s->tasks[idx].w = w;
	s->tasks[idx].h = h;
	s->tasks[idx].m = m;
	s->tasks[idx].use_thread = thr;
	s->tasks[idx].is_urgent = is_urgent;
	s->tasks[idx].policy = pol;
	s->tasks[idx].is_running_now = 0;
	s->tasks[idx].func = f;
	s->tasks[idx].arg = a;

	pthread_cond_signal(&s->wakeup_cond);
	pthread_mutex_unlock(&s->lock);

	return id;
}

void scheduler_remove_task(struct scheduler *s, uint64_t id)
{
	struct task *t;

	pthread_mutex_lock(&s->lock);
	t = find_task(s, id);
	if (t) {
		t->is_active = 0;
	}
	pthread_mutex_unlock(&s->lock);
}

/* ========================================================================= *
 * [6] 테스트 코드 및 사용 예제
 * ========================================================================= */

static atomic_int success_count = 0;

void get_current_time_str(char *buf, size_t size)
{
	time_t now = time(NULL);
	struct tm t;
	const char *wday_name[] = {"일", "월", "화", "수", "목", "금", "토"};

	localtime_r(&now, &t);
	snprintf(buf, size, "%04d-%02d-%02d(%s) %02d:%02d:%02d",
			t.tm_year + 1900, t.tm_mon + 1, t.tm_mday,
			wday_name[t.tm_wday],
			t.tm_hour, t.tm_min, t.tm_sec);
}

void task_example_log(struct task_context *ctx)
{
	char time_str[64];

	get_current_time_str(time_str, sizeof(time_str));
	printf("[%s] 📅 [캘린더 알림] %s (ID:%lu, Thread:%lu)\n",
			time_str, (char *)ctx->user_arg, ctx->task_id, pthread_self());
}

void task_verify_success(struct task_context *ctx)
{
	char time_str[64];

	get_current_time_str(time_str, sizeof(time_str));
	atomic_fetch_add(&success_count, 1);
	printf("[%s] 🟢 [검증 작업] 실행 완료! (ID:%lu, 현재 카운트: %d)\n",
			time_str, ctx->task_id, atomic_load(&success_count));
}

int main(void)
{
	struct scheduler s;
	uint64_t c_id;
	int final_count, mem_leaks, is_success;

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

	scheduler_add_calendar(&s, DAY_MON, 9, 0, 1, 0, POLICY_OVERLAP, task_example_log, "월/수/금 09:00 시스템 점검 (월)");
	scheduler_add_calendar(&s, DAY_WED, 9, 0, 1, 0, POLICY_OVERLAP, task_example_log, "월/수/금 09:00 시스템 점검 (수)");
	scheduler_add_calendar(&s, DAY_FRI, 9, 0, 1, 0, POLICY_OVERLAP, task_example_log, "월/수/금 09:00 시스템 점검 (금)");

	//scheduler_add_periodic(&s, 5000, 1, 0, POLICY_OVERLAP, task_example_log, "⏱️ 5초 주기 핑(Ping) 테스트");

	printf("  -> ✅ 다양한 캘린더 예약이 큐에 안전하게 등록되었습니다.\n\n");
	sleep(60*60*24*8);

	printf("📝 [Part 2] 스케줄러 코어 엔진 자동 검증 시작\n");
	atomic_store(&success_count, 0);

	scheduler_add_oneshot(&s, 0, 1, task_verify_success, NULL);
	scheduler_add_oneshot(&s, 100, 0, task_verify_success, NULL);
	scheduler_add_oneshot(&s, 300, 0, task_verify_success, NULL);
	scheduler_add_periodic(&s, 500, 1, 0, POLICY_OVERLAP, task_verify_success, NULL);

	c_id = scheduler_add_oneshot(&s, 200, 0, task_verify_success, NULL);
	scheduler_remove_task(&s, c_id);

	printf("\n⏳ 검증 작업들이 완료될 때까지 1초간 대기합니다...\n\n");
	sleep(1);

	printf("\n🛑 스케줄러 종료 시퀀스 시작...\n");
	scheduler_stop(&s, 5);

	printf("\n======================================================\n");
	printf("📊 테스트 결과 검증 리포트\n");
	printf("======================================================\n");

	final_count = atomic_load(&success_count);
	mem_leaks = atomic_load(&internal_alloc_count);
	is_success = 1;

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

	if (is_success) {
		printf("\n🎉 [ALL TESTS PASSED] 축하합니다! 스케줄러가 완벽하게 동작합니다!\n\n");
	} else {
		printf("\n⚠️ [TEST FAILED] 버그가 발견되었습니다.\n\n");
	}

	return 0;
}
