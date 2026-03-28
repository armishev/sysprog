#include "thread_pool.h"

#include <cerrno>
#include <cmath>
#include <ctime>

#include <deque>
#include <pthread.h>
#include <vector>

struct thread_task {
	thread_task_f function;
	pthread_mutex_t state_mu;
	pthread_cond_t completion_cv;
	bool ever_pushed;
	bool running_body;
	bool execution_done;
	bool join_completed;
#if NEED_DETACH
	bool detached;
	int join_pending;
	pthread_cond_t drain_cv;
#endif
};

static void
timespec_add_seconds(struct timespec *ts, double sec)
{
	time_t add_s = static_cast<time_t>(std::floor(sec));
	long add_ns = static_cast<long>((sec - std::floor(sec)) * 1e9);
	ts->tv_sec += add_s;
	ts->tv_nsec += add_ns;
	if (ts->tv_nsec >= 1000000000L) {
		ts->tv_sec += ts->tv_nsec / 1000000000L;
		ts->tv_nsec %= 1000000000L;
	}
}

struct thread_pool {
	int max_threads;
	std::vector<pthread_t> threads;
	std::deque<thread_task *> queue;
	pthread_mutex_t pool_mu;
	pthread_cond_t work_cv;
	bool shutdown;
	int running;
	int threads_waiting;
};

static void *
worker_main(void *arg)
{
	struct thread_pool *pool = static_cast<struct thread_pool *>(arg);

	for (;;) {
		pthread_mutex_lock(&pool->pool_mu);
		pool->threads_waiting++;
		while (!pool->shutdown && pool->queue.empty())
			pthread_cond_wait(&pool->work_cv, &pool->pool_mu);
		pool->threads_waiting--;

		if (pool->shutdown && pool->queue.empty()) {
			pthread_mutex_unlock(&pool->pool_mu);
			return NULL;
		}
		struct thread_task *task = pool->queue.front();
		pool->queue.pop_front();
		pool->running++;
		pthread_mutex_unlock(&pool->pool_mu);

		pthread_mutex_lock(&task->state_mu);
		task->running_body = true;
		pthread_mutex_unlock(&task->state_mu);

		task->function();

		pthread_mutex_lock(&pool->pool_mu);
		pool->running--;
		pthread_mutex_unlock(&pool->pool_mu);

		pthread_mutex_lock(&task->state_mu);
		task->running_body = false;
		task->execution_done = true;
		pthread_cond_broadcast(&task->completion_cv);
#if NEED_DETACH
		if (task->detached) {
			while (task->join_pending > 0)
				pthread_cond_wait(&task->drain_cv, &task->state_mu);
		}
#endif
		pthread_mutex_unlock(&task->state_mu);
#if NEED_DETACH
		if (task->detached) {
			pthread_mutex_destroy(&task->state_mu);
			pthread_cond_destroy(&task->completion_cv);
			pthread_cond_destroy(&task->drain_cv);
			delete task;
		}
#endif
	}
}

int
thread_pool_new(int thread_count, struct thread_pool **pool)
{
	if (thread_count <= 0 || thread_count > TPOOL_MAX_THREADS)
		return TPOOL_ERR_INVALID_ARGUMENT;

	struct thread_pool *p = new struct thread_pool();
	p->max_threads = thread_count;
	p->shutdown = false;
	p->running = 0;
	p->threads_waiting = 0;
	pthread_mutex_init(&p->pool_mu, NULL);
	pthread_cond_init(&p->work_cv, NULL);
	*pool = p;
	return 0;
}

int
thread_pool_delete(struct thread_pool *pool)
{
	if (pool == NULL)
		return 0;

	pthread_mutex_lock(&pool->pool_mu);
	if (!pool->queue.empty() || pool->running > 0) {
		pthread_mutex_unlock(&pool->pool_mu);
		return TPOOL_ERR_HAS_TASKS;
	}
	pool->shutdown = true;
	pthread_cond_broadcast(&pool->work_cv);
	pthread_mutex_unlock(&pool->pool_mu);

	for (pthread_t th : pool->threads)
		pthread_join(th, NULL);

	pthread_mutex_destroy(&pool->pool_mu);
	pthread_cond_destroy(&pool->work_cv);
	delete pool;
	return 0;
}

int
thread_pool_push_task(struct thread_pool *pool, struct thread_task *task)
{
	pthread_mutex_lock(&pool->pool_mu);
	if (pool->queue.size() + (size_t)pool->running >= TPOOL_MAX_TASKS) {
		pthread_mutex_unlock(&pool->pool_mu);
		return TPOOL_ERR_TOO_MANY_TASKS;
	}

	pthread_mutex_lock(&task->state_mu);
	task->execution_done = false;
	task->join_completed = false;
	task->ever_pushed = true;
#if NEED_DETACH
	task->detached = false;
#endif
	pthread_mutex_unlock(&task->state_mu);

	pool->queue.push_back(task);
	pthread_cond_signal(&pool->work_cv);

	if (!pool->shutdown && pool->threads_waiting == 0 &&
	    (int)pool->threads.size() < pool->max_threads) {
		pthread_t th;
		if (pthread_create(&th, NULL, worker_main, pool) == 0)
			pool->threads.push_back(th);
	}

	pthread_mutex_unlock(&pool->pool_mu);
	return 0;
}

int
thread_task_new(struct thread_task **task, const thread_task_f &function)
{
	struct thread_task *t = new struct thread_task();
	t->function = function;
	pthread_mutex_init(&t->state_mu, NULL);
	pthread_cond_init(&t->completion_cv, NULL);
	t->ever_pushed = false;
	t->running_body = false;
	t->execution_done = false;
	t->join_completed = false;
#if NEED_DETACH
	t->detached = false;
	t->join_pending = 0;
	pthread_cond_init(&t->drain_cv, NULL);
#endif
	*task = t;
	return 0;
}

bool
thread_task_is_finished(const struct thread_task *task)
{
	pthread_mutex_lock(const_cast<pthread_mutex_t *>(&task->state_mu));
	bool r = task->execution_done && task->join_completed;
	pthread_mutex_unlock(const_cast<pthread_mutex_t *>(&task->state_mu));
	return r;
}

bool
thread_task_is_running(const struct thread_task *task)
{
	pthread_mutex_lock(const_cast<pthread_mutex_t *>(&task->state_mu));
	bool r = task->running_body;
	pthread_mutex_unlock(const_cast<pthread_mutex_t *>(&task->state_mu));
	return r;
}

int
thread_task_join(struct thread_task *task)
{
	pthread_mutex_lock(&task->state_mu);
	if (!task->ever_pushed) {
		pthread_mutex_unlock(&task->state_mu);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
#if NEED_DETACH
	if (task->detached) {
		pthread_mutex_unlock(&task->state_mu);
		return TPOOL_ERR_INVALID_ARGUMENT;
	}
#endif
	if (task->execution_done) {
		task->join_completed = true;
		pthread_mutex_unlock(&task->state_mu);
		return 0;
	}
#if NEED_DETACH
	task->join_pending++;
#endif
	while (!task->execution_done) {
		pthread_cond_wait(&task->completion_cv, &task->state_mu);
#if NEED_DETACH
		if (task->detached) {
			task->join_pending--;
			pthread_cond_broadcast(&task->drain_cv);
			pthread_mutex_unlock(&task->state_mu);
			return TPOOL_ERR_INVALID_ARGUMENT;
		}
#endif
	}
#if NEED_DETACH
	if (task->detached) {
		task->join_pending--;
		pthread_cond_broadcast(&task->drain_cv);
		pthread_mutex_unlock(&task->state_mu);
		return TPOOL_ERR_INVALID_ARGUMENT;
	}
	task->join_pending--;
	pthread_cond_broadcast(&task->drain_cv);
#endif
	task->join_completed = true;
	pthread_mutex_unlock(&task->state_mu);
	return 0;
}

#if NEED_TIMED_JOIN

int
thread_task_timed_join(struct thread_task *task, double timeout)
{
	pthread_mutex_lock(&task->state_mu);
	if (!task->ever_pushed) {
		pthread_mutex_unlock(&task->state_mu);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
#if NEED_DETACH
	if (task->detached) {
		pthread_mutex_unlock(&task->state_mu);
		return TPOOL_ERR_INVALID_ARGUMENT;
	}
#endif
	if (task->execution_done) {
		task->join_completed = true;
		pthread_mutex_unlock(&task->state_mu);
		return 0;
	}

	if (timeout <= 0 || std::isnan(timeout)) {
		pthread_mutex_unlock(&task->state_mu);
		return TPOOL_ERR_TIMEOUT;
	}

	if ((std::isinf(timeout) && timeout > 0) ||
	    (std::isfinite(timeout) && timeout > 1e12)) {
		pthread_mutex_unlock(&task->state_mu);
		return thread_task_join(task);
	}

	struct timespec deadline;
	clock_gettime(CLOCK_REALTIME, &deadline);
	timespec_add_seconds(&deadline, timeout);

#if NEED_DETACH
	task->join_pending++;
#endif
	while (!task->execution_done) {
#if NEED_DETACH
		if (task->detached) {
			task->join_pending--;
			pthread_cond_broadcast(&task->drain_cv);
			pthread_mutex_unlock(&task->state_mu);
			return TPOOL_ERR_INVALID_ARGUMENT;
		}
#endif
		struct timespec now;
		clock_gettime(CLOCK_REALTIME, &now);
		if (now.tv_sec > deadline.tv_sec ||
		    (now.tv_sec == deadline.tv_sec &&
		     now.tv_nsec >= deadline.tv_nsec)) {
#if NEED_DETACH
			task->join_pending--;
			pthread_cond_broadcast(&task->drain_cv);
#endif
			pthread_mutex_unlock(&task->state_mu);
			return TPOOL_ERR_TIMEOUT;
		}
		int rc = pthread_cond_timedwait(&task->completion_cv,
						&task->state_mu, &deadline);
		if (rc == ETIMEDOUT) {
#if NEED_DETACH
			task->join_pending--;
			pthread_cond_broadcast(&task->drain_cv);
#endif
			pthread_mutex_unlock(&task->state_mu);
			return TPOOL_ERR_TIMEOUT;
		}
	}
#if NEED_DETACH
	if (task->detached) {
		task->join_pending--;
		pthread_cond_broadcast(&task->drain_cv);
		pthread_mutex_unlock(&task->state_mu);
		return TPOOL_ERR_INVALID_ARGUMENT;
	}
	task->join_pending--;
	pthread_cond_broadcast(&task->drain_cv);
#endif
	task->join_completed = true;
	pthread_mutex_unlock(&task->state_mu);
	return 0;
}

#endif

int
thread_task_delete(struct thread_task *task)
{
	pthread_mutex_lock(&task->state_mu);
	if (task->ever_pushed && !task->join_completed) {
		pthread_mutex_unlock(&task->state_mu);
		return TPOOL_ERR_TASK_IN_POOL;
	}
	pthread_mutex_unlock(&task->state_mu);

	pthread_mutex_destroy(&task->state_mu);
	pthread_cond_destroy(&task->completion_cv);
#if NEED_DETACH
	pthread_cond_destroy(&task->drain_cv);
#endif
	delete task;
	return 0;
}

#if NEED_DETACH

int
thread_task_detach(struct thread_task *task)
{
	pthread_mutex_lock(&task->state_mu);
	if (!task->ever_pushed) {
		pthread_mutex_unlock(&task->state_mu);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	if (task->execution_done) {
		while (task->join_pending > 0)
			pthread_cond_wait(&task->drain_cv, &task->state_mu);
		pthread_mutex_unlock(&task->state_mu);
		pthread_mutex_destroy(&task->state_mu);
		pthread_cond_destroy(&task->completion_cv);
		pthread_cond_destroy(&task->drain_cv);
		delete task;
		return 0;
	}
	if (task->detached) {
		pthread_mutex_unlock(&task->state_mu);
		return TPOOL_ERR_INVALID_ARGUMENT;
	}
	task->detached = true;
	pthread_mutex_unlock(&task->state_mu);
	return 0;
}

#endif
