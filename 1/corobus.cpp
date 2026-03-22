#include "corobus.h"

#include "libcoro.h"
#include "rlist.h"

#include <algorithm>
#include <assert.h>
#include <deque>

/**
 * One coroutine waiting to be woken up in a list of other
 * suspended coros.
 */
struct wakeup_entry {
	struct rlist base;
	struct coro *coro;
};

/** A queue of suspended coros waiting to be woken up. */
struct wakeup_queue {
	struct rlist coros;
};

/** Suspend the current coroutine until it is woken up. */
static void
wakeup_queue_suspend_this(struct wakeup_queue *queue)
{
	struct wakeup_entry entry;
	entry.coro = coro_this();
	rlist_add_tail_entry(&queue->coros, &entry, base);
	coro_suspend();
	rlist_del_entry(&entry, base);
}

/** Wakeup the first coroutine in the queue. */
static void
wakeup_queue_wakeup_first(struct wakeup_queue *queue)
{
	if (rlist_empty(&queue->coros))
		return;
	struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
		struct wakeup_entry, base);
	coro_wakeup(entry->coro);
}

static void
wakeup_queue_wakeup_all(struct wakeup_queue *queue)
{
	struct wakeup_entry *item;
	struct wakeup_entry *tmp;
	rlist_foreach_entry_safe(item, &queue->coros, base, tmp)
		coro_wakeup(item->coro);
}

struct coro_bus;

struct coro_bus_channel {
	struct coro_bus *bus;
	size_t size_limit;
	struct wakeup_queue send_queue;
	struct wakeup_queue recv_queue;
	std::deque<unsigned> messages;
};

struct coro_bus {
	struct coro_bus_channel **channels;
	int channel_count;
	int open_count;
	struct rlist broadcast_wait;
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

static void
broadcast_wakeup_all(struct coro_bus *bus);

static struct coro_bus_channel *
get_channel(struct coro_bus *bus, int channel)
{
	if (channel < 0 || channel >= bus->channel_count)
		return NULL;
	return bus->channels[channel];
}

static struct coro_bus_channel *
channel_new(struct coro_bus *bus, size_t size_limit)
{
	struct coro_bus_channel *ch = new coro_bus_channel();
	ch->bus = bus;
	ch->size_limit = size_limit;
	rlist_create(&ch->send_queue.coros);
	rlist_create(&ch->recv_queue.coros);
	return ch;
}

static void
channel_close(struct coro_bus *bus, int channel_id)
{
	struct coro_bus_channel *ch = bus->channels[channel_id];
	assert(ch != NULL);

	bus->channels[channel_id] = NULL;
	bus->open_count--;
	ch->messages.clear();

	wakeup_queue_wakeup_all(&ch->send_queue);
	wakeup_queue_wakeup_all(&ch->recv_queue);
	broadcast_wakeup_all(bus);

	while (!rlist_empty(&ch->send_queue.coros) ||
	       !rlist_empty(&ch->recv_queue.coros))
		coro_yield();

	delete ch;
}

static void
broadcast_wakeup_all(struct coro_bus *bus)
{
	struct wakeup_entry *item;
	struct wakeup_entry *tmp;
	rlist_foreach_entry_safe(item, &bus->broadcast_wait, base, tmp)
		coro_wakeup(item->coro);
}

enum coro_bus_error_code
coro_bus_errno(void)
{
	return global_error;
}

void
coro_bus_errno_set(enum coro_bus_error_code err)
{
	global_error = err;
}

struct coro_bus *
coro_bus_new(void)
{
	struct coro_bus *bus = new coro_bus;
	bus->channels = NULL;
	bus->channel_count = 0;
	bus->open_count = 0;
	rlist_create(&bus->broadcast_wait);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return bus;
}

void
coro_bus_delete(struct coro_bus *bus)
{
	if (bus == NULL)
		return;
	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels[i] != NULL)
			channel_close(bus, i);
	}
	delete[] bus->channels;
	delete bus;
}

int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit)
{
	int idx = -1;
	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels[i] == NULL) {
			idx = i;
			break;
		}
	}
	if (idx < 0) {
		int old_count = bus->channel_count;
		int new_count = old_count == 0 ? 4 : old_count * 2;
		struct coro_bus_channel **arr =
			new coro_bus_channel *[new_count];
		for (int i = 0; i < new_count; ++i)
			arr[i] = NULL;
		for (int i = 0; i < old_count; ++i)
			arr[i] = bus->channels[i];
		delete[] bus->channels;
		bus->channels = arr;
		bus->channel_count = new_count;
		idx = old_count;
	}

	struct coro_bus_channel *ch = channel_new(bus, size_limit);
	bus->channels[idx] = ch;
	bus->open_count++;
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return idx;
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
	struct coro_bus_channel *ch = get_channel(bus, channel);
	assert(ch != NULL);
	(void)ch;
	channel_close(bus, channel);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
}

int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{
	struct coro_bus_channel *ch = get_channel(bus, channel);
	if (ch == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	if (ch->messages.size() >= ch->size_limit) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	ch->messages.push_back(data);
	wakeup_queue_wakeup_first(&ch->recv_queue);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}

int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data)
{
	for (;;) {
		int rc = coro_bus_try_send(bus, channel, data);
		if (rc == 0) {
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			return 0;
		}
		if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL)
			return -1;
		struct coro_bus_channel *ch = get_channel(bus, channel);
		if (ch == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		wakeup_queue_suspend_this(&ch->send_queue);
	}
}

int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	struct coro_bus_channel *ch = get_channel(bus, channel);
	if (ch == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	if (ch->messages.empty()) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	*data = ch->messages.front();
	ch->messages.pop_front();
	wakeup_queue_wakeup_first(&ch->send_queue);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	broadcast_wakeup_all(ch->bus);
	return 0;
}

int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	for (;;) {
		int rc = coro_bus_try_recv(bus, channel, data);
		if (rc == 0) {
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			return 0;
		}
		if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL)
			return -1;
		struct coro_bus_channel *ch = get_channel(bus, channel);
		if (ch == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		wakeup_queue_suspend_this(&ch->recv_queue);
	}
}

#if NEED_BROADCAST

int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data)
{
	if (bus->open_count == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	for (int i = 0; i < bus->channel_count; ++i) {
		struct coro_bus_channel *ch = bus->channels[i];
		if (ch == NULL)
			continue;
		if (ch->messages.size() >= ch->size_limit) {
			coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
			return -1;
		}
	}
	for (int i = 0; i < bus->channel_count; ++i) {
		struct coro_bus_channel *ch = bus->channels[i];
		if (ch == NULL)
			continue;
		ch->messages.push_back(data);
		wakeup_queue_wakeup_first(&ch->recv_queue);
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	broadcast_wakeup_all(bus);
	return 0;
}

int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
	for (;;) {
		int rc = coro_bus_try_broadcast(bus, data);
		if (rc == 0) {
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			return 0;
		}
		if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL)
			return -1;
		struct wakeup_entry entry;
		entry.coro = coro_this();
		rlist_add_tail_entry(&bus->broadcast_wait, &entry, base);
		coro_suspend();
		rlist_del_entry(&entry, base);
	}
}

#endif

#if NEED_BATCH

int
coro_bus_try_send_v(struct coro_bus *bus, int channel,
	const unsigned *data, unsigned count)
{
	struct coro_bus_channel *ch = get_channel(bus, channel);
	if (ch == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	if (count == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_NONE);
		return 0;
	}
	size_t free_slots = ch->size_limit - ch->messages.size();
	if (free_slots == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	unsigned k = (unsigned)std::min((size_t)count, free_slots);
	for (unsigned i = 0; i < k; ++i)
		ch->messages.push_back(data[i]);
	for (unsigned i = 0; i < k; ++i)
		wakeup_queue_wakeup_first(&ch->recv_queue);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return (int)k;
}

int
coro_bus_send_v(struct coro_bus *bus, int channel,
	const unsigned *data, unsigned count)
{
	for (;;) {
		if (count == 0) {
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			return 0;
		}
		int k = coro_bus_try_send_v(bus, channel, data, count);
		if (k > 0) {
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			return k;
		}
		if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL)
			return -1;
		struct coro_bus_channel *ch = get_channel(bus, channel);
		if (ch == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		wakeup_queue_suspend_this(&ch->send_queue);
	}
}

int
coro_bus_try_recv_v(struct coro_bus *bus, int channel,
	unsigned *data, unsigned capacity)
{
	struct coro_bus_channel *ch = get_channel(bus, channel);
	if (ch == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	if (capacity == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_NONE);
		return 0;
	}
	if (ch->messages.empty()) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	unsigned k = (unsigned)std::min((size_t)capacity, ch->messages.size());
	for (unsigned i = 0; i < k; ++i) {
		data[i] = ch->messages.front();
		ch->messages.pop_front();
		wakeup_queue_wakeup_first(&ch->send_queue);
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	broadcast_wakeup_all(ch->bus);
	return (int)k;
}

int
coro_bus_recv_v(struct coro_bus *bus, int channel,
	unsigned *data, unsigned capacity)
{
	for (;;) {
		if (capacity == 0) {
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			return 0;
		}
		int k = coro_bus_try_recv_v(bus, channel, data, capacity);
		if (k > 0) {
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			if ((unsigned)k < capacity)
				coro_yield();
			return k;
		}
		if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL)
			return -1;
		struct coro_bus_channel *ch = get_channel(bus, channel);
		if (ch == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		wakeup_queue_suspend_this(&ch->recv_queue);
	}
}

#endif
