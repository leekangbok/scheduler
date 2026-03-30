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
 * [2] OS 추상화 계층 (SoftEther 스타일 Event & Queue 모델)
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

/* --- 🚨 OSAL Event --- */
typedef struct os_event {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    int state;
} os_event_t;

os_event_t *os_event_new(void)
{
    os_event_t *e = MALLOC(sizeof(os_event_t));
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
    FREE(e);
}

void os_event_set(os_event_t *e)
{
    pthread_mutex_lock(&e->lock);
    e->state = 1; 
    pthread_cond_broadcast(&e->cond);
    pthread_mutex_unlock(&e->lock);
}

static struct timespec _get_mono_timespec(long ms) {
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
        while (!e->state) {
            pthread_cond_wait(&e->cond, &e->lock);
        }
    } else {
        struct timespec ts = _get_mono_timespec(timeout_ms);
        while (!e->state) {
            if (pthread_cond_timedwait(&e->cond, &e->lock, &ts) == ETIMEDOUT) {
                if (!e->state) {
                    ret = OS_TIMEOUT;
                }
                break;
            }
        }
    }

    if (ret == OS_SUCCESS) {
        e->state = 0; 
    }

    pthread_mutex_unlock(&e->lock);
    return ret;
}

/* --- 🚨 OSAL Queue (SoftEther 스타일 범용 Ring Buffer) --- */
typedef struct os_queue {
    void **items;
    int capacity;
    int head;
    int tail;
    int count;
} os_queue_t;

os_queue_t *os_queue_new(void)
{
    os_queue_t *q = MALLOC(sizeof(os_queue_t));
    q->capacity = 16;
    q->items = MALLOC(sizeof(void *) * q->capacity);
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    return q;
}

void os_queue_free(os_queue_t *q)
{
    if (!q) return;
    FREE(q->items);
    FREE(q);
}

void os_queue_insert(os_queue_t *q, void *p)
{
    int i, new_cap;
    void **new_items;

    if (q->count == q->capacity) {
        /* 큐가 가득 차면 용량을 2배로 늘리고 환형 버퍼를 일렬로 폅니다. */
        new_cap = q->capacity * 2;
        new_items = MALLOC(sizeof(void *) * new_cap);
        for (i = 0; i < q->count; i++) {
            new_items[i] = q->items[(q->head + i) % q->capacity];
        }
        FREE(q->items);
        q->items = new_items;
        q->head = 0;
        q->tail = q->count;
        q->capacity = new_cap;
    }

    q->items[q->tail] = p;
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;
}

void *os_queue_get(os_queue_t *q)
{
    void *p;

    if (q->count == 0) {
        return NULL;
    }

    p = q->items[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->count--;

    return p;
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
struct task_context;

typedef void (*task_func_t)(struct task_context *ctx);

struct task_context {
    struct scheduler *sched;
    void *user_arg;
    uint64_t task_id;
};

/* 비즈니스 로직(Job) 구조체. next 포인터가 완전히 제거되었습니다! */
struct job_item {
    task_func_t func;
    void *arg;
    uint64_t task_id;
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
    
    os_mutex_t lock;
    volatile int is_shutting_down;
    os_thread_t scheduler_thread;
    
    os_event_t *wakeup_event;
    os_event_t *job_event;    
    os_queue_t *job_queue;    /* 🚨 새롭게 추가된 범용 큐 객체 🚨 */
    
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
 * [4] 유틸리티 및 시간 계산
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

        t.tm_isdst = -1; 
        next_t = mktime(&t);
    }

    if (tw >= 0) {
        if (t.tm_wday != tw) {
            diff = (tw - t.tm_wday + 7) % 7;
            t.tm_mday += diff;

            if (th < 0) {
                t.tm_hour = 0;
            }

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

    os_mutex_lock(&s->lock);
    s->active_jobs--;

    t = find_task(s, task_id);
    if (t) {
        t->is_running_now--;
    }

    if ((t && t->is_running_now == 0) || s->active_jobs == 0) {
        os_event_set(s->wakeup_event);
    }

    os_mutex_unlock(&s->lock);
    FREE(uargs);

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

        /* 1. 데이터(큐)에 접근할 때만 락을 사용합니다 */
        os_mutex_lock(&s->lock);
        
        /* 🚨 범용 os_queue_get 을 통해 아이템을 꺼냅니다 🚨 */
        job = (struct job_item *)os_queue_get(s->job_queue);
        
        if (job) {
            s->queued_jobs--;
            s->busy_workers++;
            s->active_jobs++;
            
            /* 큐에 작업이 더 있다면 동료 워커를 깨웁니다 */
            if (s->job_queue->count > 0) {
                os_event_set(s->job_event);
            }
        }
        os_mutex_unlock(&s->lock);

        /* 2. 꺼내온 작업 실행 */
        if (job) {
            ctx.sched = s;
            ctx.user_arg = job->arg;
            ctx.task_id = job->task_id;
            job->func(&ctx);

            os_mutex_lock(&s->lock);
            s->busy_workers--;
            s->active_jobs--;

            t = find_task(s, job->task_id);
            if (t) {
                t->is_running_now--;
            }

            if ((t && t->is_running_now == 0) || s->active_jobs == 0) {
                os_event_set(s->wakeup_event);
            }
            os_mutex_unlock(&s->lock);

            FREE(job);
            continue; /* 다음 작업을 확인하러 갑니다 */
        }

        /* 3. 종료 명령 확인 */
        if (s->is_shutting_down) {
            os_mutex_lock(&s->lock);
            s->cur_workers--;
            os_event_set(s->wakeup_event);
            os_mutex_unlock(&s->lock);
            break;
        }

        /* 4. 대기 */
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

    while (!s->is_shutting_down) {
        os_mutex_lock(&s->lock);

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

                            if (os_thread_create(&tid, urgent_worker_proc, uargs) == 0) {
                                os_thread_detach(tid);
                            } else {
                                t->is_running_now--;
                                s->active_jobs--;
                                FREE(uargs);
                            }
                        } else {
                            /* 🚨 연결 리스트 노드 생성 로직이 완전히 사라졌습니다! 🚨 */
                            j = MALLOC(sizeof(struct job_item));
                            j->func = t->func;
                            j->arg = t->arg;
                            j->task_id = t->id;
                            
                            os_queue_insert(s->job_queue, j); /* 범용 큐에 넣기만 하면 끝 */
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
                        ctx.user_arg = t->arg;
                        ctx.task_id = t->id;
                        task_func_t safe_func = t->func;

                        os_mutex_unlock(&s->lock);

                        safe_func(&ctx);

                        os_mutex_lock(&s->lock);

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
        os_mutex_unlock(&s->lock);

        if (!s->is_shutting_down) {
            now_mono = timespec_now_monotonic();
            timeout_ms = (next_w.tv_sec - now_mono.tv_sec) * 1000 + 
                         (next_w.tv_nsec - now_mono.tv_nsec) / 1000000;
            
            if (timeout_ms <= 0) {
                timeout_ms = 1;
            }

            os_event_wait(s->wakeup_event, timeout_ms);
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
    s->capacity = INITIAL_CAPACITY;
    s->tasks = MALLOC(sizeof(struct task) * s->capacity);
    s->next_task_id = 1;

    os_mutex_init(&s->lock);
    s->wakeup_event = os_event_new();
    s->job_event = os_event_new();
    s->job_queue = os_queue_new(); /* 🚨 큐 객체 초기화 */
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

void scheduler_stop(struct scheduler *s, int timeout_sec)
{
    struct job_item *job;
    long elapsed = 0;
    int i;

    os_mutex_lock(&s->lock);
    s->is_shutting_down = 1;
    os_mutex_unlock(&s->lock);

    os_event_set(s->wakeup_event);
    for (i = 0; i < MAX_WORKERS; i++) {
        os_event_set(s->job_event);
    }

    os_thread_join(s->scheduler_thread);

    while (elapsed < timeout_sec * 1000) {
        os_mutex_lock(&s->lock);
        int remaining = s->active_jobs + s->cur_workers;
        os_mutex_unlock(&s->lock);

        if (remaining == 0) {
            break;
        }

        os_event_wait(s->wakeup_event, 100);
        elapsed += 100;
        
        os_event_set(s->job_event); 
    }

    os_mutex_lock(&s->lock);
    if (s->active_jobs > 0 || s->cur_workers > 0) {
        printf("\n[Warning] 타임아웃 초과! 워커 스레드가 아직 실행 중이므로 메모리 강제 해제를 스킵합니다.\n");
        os_mutex_unlock(&s->lock);
        return;
    }

    /* 🚨 큐에 남은 메모리를 아주 깔끔하게 비웁니다 🚨 */
    while ((job = (struct job_item *)os_queue_get(s->job_queue)) != NULL) {
        FREE(job);
    }
    os_queue_free(s->job_queue);

    FREE(s->tasks);
    os_mutex_unlock(&s->lock);

    printf("\n[Memory Report] 최종 할당 카운트: %d (0이면 정상)\n", atomic_load(&internal_alloc_count));

    os_mutex_destroy(&s->lock);
    os_event_free(s->wakeup_event);
    os_event_free(s->job_event);
}

uint64_t scheduler_add_oneshot(struct scheduler *s, long delay, int is_urgent, task_func_t f, void *a)
{
    int idx;
    uint64_t id;

    os_mutex_lock(&s->lock);
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

    os_event_set(s->wakeup_event);
    os_mutex_unlock(&s->lock);

    return id;
}

uint64_t scheduler_add_periodic(struct scheduler *s, long interval, int thr, int is_urgent,
        enum overrun_policy pol, task_func_t f, void *a)
{
    int idx;
    uint64_t id;

    os_mutex_lock(&s->lock);
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

    os_event_set(s->wakeup_event);
    os_mutex_unlock(&s->lock);

    return id;
}

uint64_t scheduler_add_calendar(struct scheduler *s, int w, int h, int m, int thr, int is_urgent,
        enum overrun_policy pol, task_func_t f, void *a)
{
    int idx;
    uint64_t id;

    os_mutex_lock(&s->lock);
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

    os_event_set(s->wakeup_event);
    os_mutex_unlock(&s->lock);

    return id;
}

void scheduler_remove_task(struct scheduler *s, uint64_t id)
{
    struct task *t;

    os_mutex_lock(&s->lock);
    t = find_task(s, id);
    if (t) {
        t->is_active = 0;
    }
    os_mutex_unlock(&s->lock);
}

/* ========================================================================= *
 * [7] 테스트 코드 및 시스템 종료 시그널 핸들러
 * ========================================================================= */

static atomic_int success_count = 0;
static struct scheduler *g_sched = NULL;

void handle_sigint(int sig)
{
    printf("\n[Signal] %d 신호 수신! 스케줄러 안전 종료 시퀀스 시작...\n", sig);
    if (g_sched) {
        scheduler_stop(g_sched, 5); 
    }
    exit(0);
}

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
            time_str, (char *)ctx->user_arg, ctx->task_id, os_thread_get_id());
}

void task_verify_success(struct task_context *ctx)
{
    char time_str[64];

    get_current_time_str(time_str, sizeof(time_str));
    atomic_fetch_add(&success_count, 1);
    printf("[%s] 🟢 [검증 작업] 실행 완료! (ID:%lu, 현재 카운트: %d)\n",
            time_str, ctx->task_id, atomic_load(&success_count));
}

void task_verify_ping(struct task_context *ctx)
{
    char time_str[64];

    get_current_time_str(time_str, sizeof(time_str));
    printf("[%s] 🟢 [PING 작업] 실행 완료! (ID:%lu)\n",
            time_str, ctx->task_id);
}

int main(void)
{
    struct scheduler s;
    uint64_t c_id;
    int final_count, mem_leaks, is_success;

    g_sched = &s;
    signal(SIGINT, handle_sigint);
    signal(SIGTERM, handle_sigint);

    scheduler_init(&s);
    scheduler_start(&s);

    printf("\n======================================================\n");
    printf("🚀 OSAL Event & Queue 기반 스케줄러 구동 시작\n");
    printf("======================================================\n\n");

    printf("📝 [Part 1] 실무 달력(Calendar) 예약 예제 등록 중...\n");

    scheduler_add_calendar(&s, DAY_ANY, TIME_ANY, 0, 1, 0, POLICY_OVERLAP, task_example_log, "매시 정각(00분) 데이터 동기화");
    scheduler_add_calendar(&s, DAY_MON, 11, 5, 1, 0, POLICY_OVERLAP, task_example_log, "매주 월요일 11:00 주간 DB 백업");
    scheduler_add_calendar(&s, DAY_MON, 0, 0, 1, 0, POLICY_OVERLAP, task_example_log, "매주 월요일 00:00 주간 DB 백업");

    scheduler_add_calendar(&s, DAY_MON, 12, 0, 1, 0, POLICY_OVERLAP, task_example_log, "월/수/금 12:00 시스템 점검 (월)");
    scheduler_add_calendar(&s, DAY_WED, 12, 0, 1, 0, POLICY_OVERLAP, task_example_log, "월/수/금 12:00 시스템 점검 (수)");
    scheduler_add_calendar(&s, DAY_FRI, 12, 0, 1, 0, POLICY_OVERLAP, task_example_log, "월/수/금 12:00 시스템 점검 (금)");
    
	scheduler_add_periodic(&s, 5*1000, 1, 0, POLICY_SKIP, task_verify_ping, NULL);

    printf("  -> ✅ 다양한 캘린더 예약이 큐에 안전하게 등록되었습니다.\n\n");

	sleep(60*60*60);

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

    // 🚨 롱런 테스트를 원하시면 아래 주석을 해제하세요.
    // printf("\n⏳ 백그라운드 대기 모드 돌입... (강제 종료하려면 Ctrl+C 누르세요)\n\n");
    // while (1) { sleep(60); }

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
