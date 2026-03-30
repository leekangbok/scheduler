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
#include <signal.h>

/* ========================================================================= *
 * [1] OS 메모리 할당기 (Categorized Memory Allocator)
 * ========================================================================= */

#define os_strcpy(dest, src) snprintf((dest), sizeof(dest), "%s", (src) ? (src) : "")
#define os_sprint(dest, fmt, ...) snprintf((dest), sizeof(dest), (fmt), ##__VA_ARGS__)

typedef struct os_allocator {
	const char *name;
	atomic_int cur_allocs;
	atomic_int tot_allocs;
} os_allocator_t;

static inline void os_allocator_init(os_allocator_t *alloc, const char *name)
{
	alloc->name = name;
	atomic_init(&alloc->cur_allocs, 0);
	atomic_init(&alloc->tot_allocs, 0);
}

static inline void *os_malloc(os_allocator_t *alloc, size_t size)
{
	if (size == 0) return NULL;
	if (alloc) {
		atomic_fetch_add(&alloc->cur_allocs, 1);
		atomic_fetch_add(&alloc->tot_allocs, 1);
	}
	return calloc(1, size); /* 무조건 0으로 자동 초기화 (Zero-page 최적화) */
}

static inline void os_free(os_allocator_t *alloc, void *ptr)
{
	if (!ptr) return;
	free(ptr);
	if (alloc) atomic_fetch_sub(&alloc->cur_allocs, 1);
}

static inline void *os_realloc(os_allocator_t *alloc, void *ptr, size_t size)
{
	if (!ptr) return os_malloc(alloc, size);
	if (size == 0) { os_free(alloc, ptr); return NULL; }
	return realloc(ptr, size); /* realloc은 블록 단위만 변경되므로 카운트 증감 없음 */
}

/* ========================================================================= *
 * [2] OS 추상화 계층 (Event & Ref-counted Queue 모델)
 * ========================================================================= */
#define OS_TIMEOUT -1
#define OS_SUCCESS 0

typedef pthread_mutex_t os_mutex_t;
typedef pthread_t       os_thread_t;

static inline void os_mutex_init(os_mutex_t *m) { pthread_mutex_init(m, NULL); }
static inline void os_mutex_lock(os_mutex_t *m) { pthread_mutex_lock(m); }
static inline void os_mutex_unlock(os_mutex_t *m) { pthread_mutex_unlock(m); }
static inline void os_mutex_destroy(os_mutex_t *m) { pthread_mutex_destroy(m); }

static inline int os_thread_create(os_thread_t *t, void *(*func)(void *), void *arg) { return pthread_create(t, NULL, func, arg); }
static inline void os_thread_detach(os_thread_t t) { pthread_detach(t); }
static inline void os_thread_join(os_thread_t t) { pthread_join(t, NULL); }
static inline unsigned long os_thread_get_id(void) { return (unsigned long)pthread_self(); }

/* --- OSAL 최상위 객체 헤더 (스스로 할당자를 기억함!) --- */
typedef struct os_object {
	atomic_int ref_count;
	os_allocator_t *alloc; /* 🚨 객체가 태어난 고향(할당자)을 기억합니다 */
	void (*dtor)(void *obj);
} os_object_t;

static inline void os_obj_init(void *obj, os_allocator_t *alloc, void (*dtor)(void *))
{
	os_object_t *o = (os_object_t *)obj;
	atomic_init(&o->ref_count, 1);
	o->alloc = alloc;
	o->dtor = dtor;
}

static inline void *os_obj_hold(void *obj)
{
	os_object_t *o = (os_object_t *)obj;
	if (o) atomic_fetch_add(&o->ref_count, 1);
	return obj;
}

static inline void os_obj_release(void *obj)
{
	os_object_t *o = (os_object_t *)obj;
	if (o && atomic_fetch_sub(&o->ref_count, 1) == 1) {
		os_allocator_t *my_alloc = o->alloc; /* dtor 실행 후 메모리가 날아갈 수 있으므로 미리 백업 */
		if (o->dtor) {
			o->dtor(o); /* 자식 리소스만 정리 (FREE 호출 금지) */
		}
		os_free(my_alloc, o); /* 🚨 스스로 할당자에게 메모리를 반환합니다! */
	}
}

/* --- OSAL Event --- */
typedef struct os_event {
	os_allocator_t *alloc;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	int state;
} os_event_t;

os_event_t *os_event_new(os_allocator_t *alloc)
{
	os_event_t *e = os_malloc(alloc, sizeof(os_event_t));
	e->alloc = alloc;
	pthread_mutex_init(&e->lock, NULL);
	pthread_condattr_t attr;
	pthread_condattr_init(&attr);
	pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
	pthread_cond_init(&e->cond, &attr);
	pthread_condattr_destroy(&attr);
	e->state = 0;
	return e;
}

void os_event_free(os_event_t *e)
{
	if (!e) return;
	pthread_mutex_destroy(&e->lock);
	pthread_cond_destroy(&e->cond);
	os_free(e->alloc, e);
}

void os_event_set(os_event_t *e)
{
	pthread_mutex_lock(&e->lock);
	e->state = 1; 
	pthread_cond_broadcast(&e->cond);
	pthread_mutex_unlock(&e->lock);
}

static struct timespec _get_mono_timespec(long ms)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	ts.tv_sec += ms / 1000;
	ts.tv_nsec += (ms % 1000) * 1000000L;
	if (ts.tv_nsec >= 1000000000L) {
		ts.tv_sec++;
		ts.tv_nsec -= 1000000000L;
	}
	return ts;
}

int os_event_wait(os_event_t *e, long timeout_ms)
{
	int ret = OS_SUCCESS;
	pthread_mutex_lock(&e->lock);

	if (timeout_ms < 0) {
		while (!e->state) pthread_cond_wait(&e->cond, &e->lock);
	} else {
		struct timespec ts = _get_mono_timespec(timeout_ms);
		while (!e->state) {
			if (pthread_cond_timedwait(&e->cond, &e->lock, &ts) == ETIMEDOUT) {
				if (!e->state) ret = OS_TIMEOUT;
				break;
			}
		}
	}

	if (ret == OS_SUCCESS) e->state = 0; 
	pthread_mutex_unlock(&e->lock);
	return ret;
}

/* --- OSAL 소유권 기반 Queue (Ref-counted Ring Buffer) --- */

#define OS_QUEUE_MIN_CAPACITY 16

typedef struct os_queue {
	os_allocator_t *alloc;
	os_mutex_t lock;
	void **items;
	int capacity;
	int head;
	int tail;
	int count;
} os_queue_t;

/* 사용자가 직접 작성한 훌륭한 매크로 유지 */
#define os_queue_item(q, i, type) \
	((type *)((q)->items[((q)->head + (i)) % (q)->capacity]))

#define os_queue_foreach(item_ptr, q, type, i) \
	for ((i) = 0; ((i) < (q)->count) && (((item_ptr) = os_queue_item(q, i, type)) || 1); (i)++)

os_queue_t *os_queue_new(os_allocator_t *alloc)
{
	os_queue_t *q = os_malloc(alloc, sizeof(os_queue_t));
	q->alloc = alloc;
	os_mutex_init(&q->lock);
	q->capacity = OS_QUEUE_MIN_CAPACITY;
	q->items = os_malloc(alloc, sizeof(void *) * q->capacity);
	q->head = 0;
	q->tail = 0;
	q->count = 0;
	return q;
}

void os_queue_insert(os_queue_t *q, void *p)
{
	int i, new_cap;
	void **new_items, *e;

	os_obj_hold(p); 

	os_mutex_lock(&q->lock);

	if (q->count == q->capacity) {
		new_cap = q->capacity * 2;
		new_items = os_malloc(q->alloc, sizeof(void *) * new_cap);
		os_queue_foreach(e, q, void, i) {
			new_items[i] = e;
		}
		os_free(q->alloc, q->items);
		q->items = new_items;
		q->head = 0;
		q->tail = q->count;
		q->capacity = new_cap;
	}

	q->items[q->tail] = p;
	q->tail = (q->tail + 1) % q->capacity;
	q->count++;

	os_mutex_unlock(&q->lock);
}

void *os_queue_pop(os_queue_t *q)
{
	void *p = NULL;
	int i, new_cap;
	void **new_items, *e;

	os_mutex_lock(&q->lock);

	if (q->count > 0) {
		p = q->items[q->head];
		q->head = (q->head + 1) % q->capacity;
		q->count--;
		if (q->capacity > OS_QUEUE_MIN_CAPACITY && q->count <= q->capacity / 4) {
			new_cap = q->capacity / 2;
			new_items = os_malloc(q->alloc, sizeof(void *) * new_cap);
			if (new_items) {
				os_queue_foreach(e, q, void, i) {
					new_items[i] = e;
				}
				os_free(q->alloc, q->items);
				q->items = new_items;
				q->head = 0;
				q->tail = q->count;
				q->capacity = new_cap;
			}
		}
	}

	os_mutex_unlock(&q->lock);
	return p; 
}

void os_queue_free(os_queue_t *q)
{
	int i;
	void *p;
	if (!q) return;

	os_mutex_lock(&q->lock);
	for (i = 0; i < q->count; i++) {
		p = q->items[(q->head + i) % q->capacity];
		os_obj_release(p); 
	}
	os_free(q->alloc, q->items);
	os_mutex_unlock(&q->lock);

	os_mutex_destroy(&q->lock);
	os_free(q->alloc, q);
}

/* ========================================================================= *
 * [3] 상수 및 자료구조
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

struct task_context {
	struct scheduler *sched;
	void *user_arg;
	uint64_t task_id;
	const char *task_name;
};

typedef void (*task_func_t)(struct task_context *ctx);

struct job_item {
	os_object_t base; 
	char name[64]; 
	task_func_t func;
	void *arg;
	uint64_t task_id;
};

struct task {
	uint64_t id;
	char name[64]; 
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
	/* 🚨 카테고리별 메모리 할당자 독립 관리 */
	struct {
		os_allocator_t core;
		os_allocator_t event;
		os_allocator_t queue;
		os_allocator_t job;
		os_allocator_t urgent;
	} allocs;

	struct task *tasks;
	int capacity;
	int task_count;
	uint64_t next_task_id;

	os_mutex_t lock;
	volatile int is_shutting_down;
	os_thread_t scheduler_thread;

	os_event_t *wakeup_event;
	os_event_t *job_event;    
	os_queue_t *job_queue;    

	int cur_workers;
	int busy_workers;
	int active_jobs;
	int queued_jobs;
};

/* ========================================================================= *
 * [4] 유틸리티 및 시간 계산
 * ========================================================================= */
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
	if (a->tv_sec != b->tv_sec) return a->tv_sec < b->tv_sec ? -1 : 1;
	if (a->tv_nsec < b->tv_nsec) return -1;
	else if (a->tv_nsec > b->tv_nsec) return 1;
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

	if (tm_min >= 0) t.tm_min = tm_min;
	if (th >= 0) t.tm_hour = th;

	next_t = mktime(&t);

	if (next_t <= now_t) {
		if (th < 0) t.tm_hour++;
		else if (tw < 0) t.tm_mday++;
		else t.tm_mday += 7;
		t.tm_isdst = -1; 
		next_t = mktime(&t);
	}

	if (tw >= 0) {
		if (t.tm_wday != tw) {
			diff = (tw - t.tm_wday + 7) % 7;
			t.tm_mday += diff;
			if (th < 0) t.tm_hour = 0;
			t.tm_isdst = -1; 
			next_t = mktime(&t);
		}
	}
	return next_t;
}

/* ========================================================================= *
 * [5] 슬롯 관리 및 코어 로직
 * ========================================================================= */
static int get_available_slot(struct scheduler *s)
{
	int i;
	for (i = 0; i < s->task_count; i++) {
		if (!s->tasks[i].is_active && s->tasks[i].is_running_now == 0) return i;
	}
	if (s->task_count >= s->capacity) {
		s->capacity *= 2;
		s->tasks = os_realloc(&s->allocs.core, s->tasks, sizeof(struct task) * s->capacity);
	}
	return s->task_count++;
}

struct task *find_task(struct scheduler *s, uint64_t id)
{
	int i;
	for (i = 0; i < s->task_count; i++) {
		if (s->tasks[i].id == id) return &s->tasks[i];
	}
	return NULL;
}

struct urgent_args {
	os_object_t base;
	struct scheduler *sched;
	struct job_item *job; 
};

void urgent_args_dtor(void *obj)
{
	struct urgent_args *uargs = (struct urgent_args *)obj;
	os_obj_release(uargs->job); 
	/* 🚨 주의: FREE(uargs) 제거됨! os_obj_release가 안전하게 os_free로 넘깁니다. */
}

void *urgent_worker_proc(void *arg)
{
	struct urgent_args *uargs = arg;
	struct scheduler *s = uargs->sched;
	struct job_item *job = uargs->job;
	struct task *t;
	struct task_context ctx = {
		.sched = s,
		.user_arg = job->arg,
		.task_id = job->task_id,
		.task_name = job->name 
	};

	job->func(&ctx);

	os_mutex_lock(&s->lock);
	s->active_jobs--;

	t = find_task(s, job->task_id);
	if (t) t->is_running_now--;
	if ((t && t->is_running_now == 0) || s->active_jobs == 0) os_event_set(s->wakeup_event);
	os_mutex_unlock(&s->lock);

	os_obj_release(uargs);
	return NULL;
}

void *worker_proc(void *arg)
{
	struct scheduler *s = arg;
	struct job_item *job;
	struct task *t;
	struct task_context ctx;

	while (1) {
		job = NULL;

		os_mutex_lock(&s->lock);
		job = (struct job_item *)os_queue_pop(s->job_queue);
		if (job) {
			s->queued_jobs--;
			s->busy_workers++;
			s->active_jobs++;
			if (s->job_queue->count > 0) os_event_set(s->job_event);
		}
		os_mutex_unlock(&s->lock);

		if (job) {
			ctx.sched = s;
			ctx.user_arg = job->arg;
			ctx.task_id = job->task_id;
			ctx.task_name = job->name; 

			job->func(&ctx);

			os_mutex_lock(&s->lock);
			s->busy_workers--;
			s->active_jobs--;

			t = find_task(s, job->task_id);
			if (t) t->is_running_now--;
			if ((t && t->is_running_now == 0) || s->active_jobs == 0) os_event_set(s->wakeup_event);
			os_mutex_unlock(&s->lock);

			os_obj_release(job);
			continue; 
		}

		if (s->is_shutting_down) {
			os_mutex_lock(&s->lock);
			s->cur_workers--;
			os_event_set(s->wakeup_event);
			os_event_set(s->job_event);
			os_mutex_unlock(&s->lock);
			break;
		}

		if (os_event_wait(s->job_event, IDLE_TIMEOUT_SEC * 1000) == OS_TIMEOUT) {
			os_mutex_lock(&s->lock);
			if (s->cur_workers > MIN_WORKERS) {
				s->cur_workers--;
				os_event_set(s->wakeup_event);
				os_mutex_unlock(&s->lock);
				break;
			}
			os_mutex_unlock(&s->lock);
		}
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
	struct job_item *j;
	struct task_context ctx;
	os_thread_t tid;
	int i, run, actual_run, idle_workers, is_waiting;
	long timeout_ms;
	char time_str[64];

	while (!s->is_shutting_down) {
		os_mutex_lock(&s->lock);

		now_mono = timespec_now_monotonic();
		now_real = time(NULL);
		next_w = timespec_add_ms(now_mono, 1000);

		for (i = 0; i < s->task_count; i++) {
			t = &s->tasks[i];

			if (!t->is_active) continue;

			run = 0;
			if (t->type == TYPE_RELATIVE) {
				if (timespec_cmp(&t->next_run, &now_mono) <= 0) run = 1;
			} else {
				if (t->target_realtime <= now_real) run = 1;
			}

			if (run) {
				actual_run = 1;
				if (t->is_running_now > 0) {
					if (t->policy == POLICY_SKIP) {
						actual_run = 0;

						get_current_time_str(time_str, sizeof(time_str));
						printf("[%s] ⚠️ [SKIP] '%s' (ID:%lu) 이전 작업 지연으로 인해 실행을 건너뜁니다!\n", time_str, t->name, t->id);

						if (t->type == TYPE_RELATIVE) {
							do {
								t->next_run = timespec_add_ms(t->next_run, t->interval_ms);
							} while (timespec_cmp(&t->next_run, &now_mono) <= 0);
						} else {
							t->target_realtime = get_next_calendar_realtime(t->w, t->h, t->m);
						}
					} else if (t->policy == POLICY_WAIT) {
						actual_run = 0;
					}
				}

				if (actual_run) {
					t->is_running_now++;

					/* 🚨 전용 할당자로 객체 생성 및 주입 */
					j = os_malloc(&s->allocs.job, sizeof(struct job_item));
					os_obj_init(j, &s->allocs.job, NULL);
					os_strcpy(j->name, t->name);
					j->func = t->func;
					j->arg = t->arg;
					j->task_id = t->id;

					if (t->use_thread) {
						if (t->is_urgent) {
							s->active_jobs++;

							uargs = os_malloc(&s->allocs.urgent, sizeof(struct urgent_args));
							os_obj_init(uargs, &s->allocs.urgent, urgent_args_dtor);
							uargs->sched = s;
							uargs->job = os_obj_hold(j);

							if (os_thread_create(&tid, urgent_worker_proc, uargs) == 0) {
								os_thread_detach(tid);
							} else {
								t->is_running_now--;
								s->active_jobs--;
								os_obj_release(uargs);
							}
							os_obj_release(j);
						} else {
							os_queue_insert(s->job_queue, j); 
							os_obj_release(j); 

							s->queued_jobs++;

							idle_workers = s->cur_workers - s->busy_workers;
							if (s->queued_jobs > idle_workers && s->cur_workers < MAX_WORKERS) {
								if (os_thread_create(&tid, worker_proc, s) == 0) {
									os_thread_detach(tid);
									s->cur_workers++;
								}
							}
							os_event_set(s->job_event);
						}
					} else {
						ctx.sched = s;
						ctx.user_arg = j->arg;
						ctx.task_id = j->task_id;
						ctx.task_name = j->name;
						task_func_t safe_func = j->func;

						os_mutex_unlock(&s->lock);
						safe_func(&ctx);
						os_mutex_lock(&s->lock);

						t = &s->tasks[i];
						t->is_running_now--;

						os_obj_release(j);
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
		os_mutex_unlock(&s->lock);

		if (!s->is_shutting_down) {
			timeout_ms = (next_w.tv_sec - now_mono.tv_sec) * 1000 + 
				(next_w.tv_nsec - now_mono.tv_nsec) / 1000000;
			if (timeout_ms > 0) {
				os_event_wait(s->wakeup_event, timeout_ms);
			}
		}
	}

	return NULL;
}

/* ========================================================================= *
 * [6] 공용 API
 * ========================================================================= */
void scheduler_init(struct scheduler *s)
{
	memset(s, 0, sizeof(struct scheduler));

	os_allocator_init(&s->allocs.core, "Core (Tasks Array)");
	os_allocator_init(&s->allocs.event, "Event Objects");
	os_allocator_init(&s->allocs.queue, "Queue Buffer");
	os_allocator_init(&s->allocs.job, "Job Items");
	os_allocator_init(&s->allocs.urgent, "Urgent Arguments");

	s->capacity = INITIAL_CAPACITY;
	s->tasks = os_malloc(&s->allocs.core, sizeof(struct task) * s->capacity);
	s->next_task_id = 1;

	os_mutex_init(&s->lock);
	s->wakeup_event = os_event_new(&s->allocs.event);
	s->job_event = os_event_new(&s->allocs.event);
	s->job_queue = os_queue_new(&s->allocs.queue); 
}

void scheduler_start(struct scheduler *s)
{
	os_thread_t tid;
	int i;

	os_mutex_lock(&s->lock);
	for (i = 0; i < MIN_WORKERS; i++) {
		if (os_thread_create(&tid, worker_proc, s) == 0) {
			os_thread_detach(tid);
			s->cur_workers++;
		}
	}
	os_mutex_unlock(&s->lock);

	os_thread_create(&s->scheduler_thread, scheduler_loop, s);
}

int scheduler_stop(struct scheduler *s, int timeout_sec)
{
	long elapsed = 0;
	int total_leaks;

	os_mutex_lock(&s->lock);
	s->is_shutting_down = 1;
	os_mutex_unlock(&s->lock);

	os_event_set(s->wakeup_event);
	os_event_set(s->job_event);

	os_thread_join(s->scheduler_thread);

	while (elapsed < timeout_sec * 1000) {
		os_mutex_lock(&s->lock);
		int remaining = s->active_jobs + s->cur_workers;
		os_mutex_unlock(&s->lock);

		if (remaining == 0) break;

		os_event_wait(s->wakeup_event, 100);
		elapsed += 100;

		os_event_set(s->job_event); 
	}

	os_mutex_lock(&s->lock);
	if (s->active_jobs > 0 || s->cur_workers > 0) {
		printf("\n[Warning] 타임아웃 초과! 워커 스레드가 아직 실행 중이므로 메모리 강제 해제를 스킵합니다.\n");
		os_mutex_unlock(&s->lock);
		return atomic_load(&s->allocs.core.cur_allocs) +
			atomic_load(&s->allocs.event.cur_allocs) +
			atomic_load(&s->allocs.queue.cur_allocs) +
			atomic_load(&s->allocs.job.cur_allocs) +
			atomic_load(&s->allocs.urgent.cur_allocs);
	}

	os_queue_free(s->job_queue);
	os_free(&s->allocs.core, s->tasks);
	os_mutex_unlock(&s->lock);

	os_mutex_destroy(&s->lock);
	os_event_free(s->wakeup_event);
	os_event_free(s->job_event);

	printf("\n======================================================\n");
	printf("🔍 [Memory Report] 카테고리별 메모리 사용 통계\n");
	printf("======================================================\n");
	printf(" - %-20s : 누적 할당 %4d회 | 현재 누수 %d Blocks\n", s->allocs.core.name,
			atomic_load(&s->allocs.core.tot_allocs), atomic_load(&s->allocs.core.cur_allocs));
	printf(" - %-20s : 누적 할당 %4d회 | 현재 누수 %d Blocks\n", s->allocs.event.name,
			atomic_load(&s->allocs.event.tot_allocs), atomic_load(&s->allocs.event.cur_allocs));
	printf(" - %-20s : 누적 할당 %4d회 | 현재 누수 %d Blocks\n", s->allocs.queue.name,
			atomic_load(&s->allocs.queue.tot_allocs), atomic_load(&s->allocs.queue.cur_allocs));
	printf(" - %-20s : 누적 할당 %4d회 | 현재 누수 %d Blocks\n", s->allocs.job.name,
			atomic_load(&s->allocs.job.tot_allocs), atomic_load(&s->allocs.job.cur_allocs));
	printf(" - %-20s : 누적 할당 %4d회 | 현재 누수 %d Blocks\n", s->allocs.urgent.name,
			atomic_load(&s->allocs.urgent.tot_allocs), atomic_load(&s->allocs.urgent.cur_allocs));

	total_leaks = atomic_load(&s->allocs.core.cur_allocs) +
		atomic_load(&s->allocs.event.cur_allocs) +
		atomic_load(&s->allocs.queue.cur_allocs) +
		atomic_load(&s->allocs.job.cur_allocs) +
		atomic_load(&s->allocs.urgent.cur_allocs);

	return total_leaks;
}

uint64_t scheduler_add_oneshot(struct scheduler *s, const char *name, long delay, int is_urgent, task_func_t f, void *a)
{
	int idx;
	uint64_t id;

	os_mutex_lock(&s->lock);
	idx = get_available_slot(s);
	id = s->next_task_id++;

	s->tasks[idx].id = id;
	if (name) {
		os_strcpy(s->tasks[idx].name, name);
	} else {
		s->tasks[idx].name[0] = '\0';
	}
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

	os_event_set(s->wakeup_event);
	os_mutex_unlock(&s->lock);

	return id;
}

uint64_t scheduler_add_periodic(struct scheduler *s, const char *name, long interval, int thr, int is_urgent,
		enum overrun_policy pol, int run_now, task_func_t f, void *a)
{
	int idx;
	uint64_t id;

	os_mutex_lock(&s->lock);
	idx = get_available_slot(s);
	id = s->next_task_id++;

	s->tasks[idx].id = id;
	if (name) {
		os_strcpy(s->tasks[idx].name, name);
	} else {
		s->tasks[idx].name[0] = '\0';
	}
	s->tasks[idx].is_active = 1;
	s->tasks[idx].is_periodic = 1;
	s->tasks[idx].type = TYPE_RELATIVE;

	if (run_now) {
		s->tasks[idx].next_run = timespec_now_monotonic();
	} else {
		s->tasks[idx].next_run = timespec_add_ms(timespec_now_monotonic(), interval);
	}

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

	os_event_set(s->wakeup_event);
	os_mutex_unlock(&s->lock);

	return id;
}

uint64_t scheduler_add_calendar(struct scheduler *s, const char *name, int w, int h, int m, int thr, int is_urgent,
		enum overrun_policy pol, int run_now, task_func_t f, void *a)
{
	int idx;
	uint64_t id;

	os_mutex_lock(&s->lock);
	idx = get_available_slot(s);
	id = s->next_task_id++;

	s->tasks[idx].id = id;
	if (name) {
		os_strcpy(s->tasks[idx].name, name);
	} else {
		s->tasks[idx].name[0] = '\0';
	}
	s->tasks[idx].is_active = 1;
	s->tasks[idx].is_periodic = 1;
	s->tasks[idx].type = TYPE_CALENDAR;

	if (run_now) {
		s->tasks[idx].target_realtime = 0; 
	} else {
		s->tasks[idx].target_realtime = get_next_calendar_realtime(w, h, m);
	}

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

	os_event_set(s->wakeup_event);
	os_mutex_unlock(&s->lock);

	return id;
}

void scheduler_remove_task(struct scheduler *s, uint64_t id)
{
	struct task *t;

	os_mutex_lock(&s->lock);
	t = find_task(s, id);
	if (t) t->is_active = 0;
	os_mutex_unlock(&s->lock);
}

/* ========================================================================= *
 * [7] 테스트 코드 및 시스템 종료 시그널 핸들러
 * ========================================================================= */

static atomic_int success_count = 0;
static struct scheduler *g_sched = NULL;

static volatile sig_atomic_t g_shutdown_flag = 0;

void handle_sigint(int sig)
{
	const char msg[] = "\n[Signal] 강제 종료 신호 수신! 스케줄러 안전 종료 시퀀스 시작...\n";
	if (write(STDOUT_FILENO, msg, sizeof(msg) - 1) < 0) { }
	g_shutdown_flag = 1;
}

void task_verify_ping(struct task_context *ctx)
{
	char time_str[64];
	get_current_time_str(time_str, sizeof(time_str));

	printf("[%s] 🟢 [PING] '%s' 시작 (ID:%lu, Thread:%lu) - 14초 딜레이 중...\n",
			time_str, ctx->task_name, ctx->task_id, os_thread_get_id());

	sleep(14); 

	get_current_time_str(time_str, sizeof(time_str));
	printf("[%s] 🟢 [PING] '%s' 종료 (ID:%lu)\n", time_str, ctx->task_name, ctx->task_id);
}

void task_example_log(struct task_context *ctx)
{
	char time_str[64];
	get_current_time_str(time_str, sizeof(time_str));
	printf("[%s] 📅 [캘린더 알림] '%s' (ID:%lu, Thread:%lu)\n",
			time_str, ctx->task_name, ctx->task_id, os_thread_get_id());
}

void task_verify_success(struct task_context *ctx)
{
	char time_str[64];
	get_current_time_str(time_str, sizeof(time_str));
	atomic_fetch_add(&success_count, 1);
	printf("[%s] 🟢 [검증 작업] '%s' 실행 완료! (ID:%lu, 현재 카운트: %d)\n",
			time_str, ctx->task_name, ctx->task_id, atomic_load(&success_count));
}

int main(void)
{
	struct scheduler s;
	uint64_t c_id;
	int final_count, is_success;

	g_sched = &s;
	signal(SIGINT, handle_sigint);
	signal(SIGTERM, handle_sigint);

	scheduler_init(&s);
	scheduler_start(&s);

	printf("\n======================================================\n");
	printf("🚀 OSAL Allocator & Ref-Count Queue 스케줄러 구동 시작\n");
	printf("======================================================\n\n");

	printf("📝 [Part 1] 코어 엔진 자동 검증 시작 (1초 소요)\n");
	atomic_store(&success_count, 0);

	scheduler_add_periodic(&s, "PING 5초", 5*1000, 1, 0, POLICY_SKIP, 1, task_verify_ping, NULL);

	scheduler_add_oneshot(&s, "1번 원샷", 0, 1, task_verify_success, NULL);
	scheduler_add_oneshot(&s, "2번 원샷", 100, 0, task_verify_success, NULL);
	scheduler_add_oneshot(&s, "3번 원샷", 300, 0, task_verify_success, NULL);
	uint64_t p_id = scheduler_add_periodic(&s, "4번 주기(700ms)", 700, 1, 0, POLICY_OVERLAP, 0, task_verify_success, NULL);

	c_id = scheduler_add_oneshot(&s, "취소될 작업", 200, 0, task_verify_success, NULL);
	scheduler_remove_task(&s, c_id);

	sleep(1);

	scheduler_remove_task(&s, p_id);

	printf("\n📝 [Part 2] 실무 달력(Calendar) 및 주기적(Periodic) 백그라운드 예약 등록\n");

	scheduler_add_calendar(&s, "매시 정각 동기화", DAY_ANY, TIME_ANY, 0, 1, 0, POLICY_OVERLAP, 0, task_example_log, NULL);
	scheduler_add_calendar(&s, "월요일 11시 백업", DAY_MON, 13, 15, 1, 0, POLICY_OVERLAP, 0, task_example_log, NULL);
	scheduler_add_calendar(&s, "즉시실행 캘린더", DAY_MON, 0, 0, 1, 0, POLICY_OVERLAP, 1, task_example_log, NULL);

	printf("  -> ✅ 다양한 예약이 큐에 안전하게 등록되었습니다.\n\n");
	printf("\n⏳ 백그라운드 대기 모드 돌입... (종료하려면 Ctrl+C 누르세요!)\n\n");

	while (!g_shutdown_flag) {
		sleep(1); 
	}

	int mem_leaks = scheduler_stop(&s, 16);

	printf("\n======================================================\n");
	printf("📊 테스트 결과 검증 리포트\n");
	printf("======================================================\n");

	final_count = atomic_load(&success_count);
	is_success = 1;

	if (final_count == 4) {
		printf("✅ 작업 실행 타이밍 및 취소 로직 검증 : [ PASS ] (예상: 4, 실제: %d)\n", final_count);
	} else {
		printf("❌ 작업 실행 타이밍 및 취소 로직 검증 : [ FAIL ] (예상: 4, 실제: %d)\n", final_count);
		is_success = 0;
	}

	/* 🚨 잃어버렸던 메모리 검증 리포트를 다시 복구했습니다! */
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
