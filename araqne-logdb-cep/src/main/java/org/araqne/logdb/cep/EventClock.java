/*
 * Copyright 2014 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb.cep;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventClock<T extends EventClockItem> {
	private final Logger slog = LoggerFactory.getLogger(EventClock.class);

	private final TimeoutComparator timeoutComparator = new TimeoutComparator();
	private final ExpireComparator expireComparator = new ExpireComparator();
	private final EventClockCallback callback;
	private final String host;
	private final PriorityQueue<Expirable<T>> timeoutQueue;
	private final HashSet<T> timeoutSet;
	private final PriorityQueue<T> expireQueue;

	private AtomicLong lastTime = new AtomicLong();

	public EventClock(EventClockCallback callback, String host, long lastTime, int initialCapacity) {
		this.callback = callback;
		this.host = host;
		this.lastTime = new AtomicLong(lastTime);
		this.timeoutQueue = new PriorityQueue<Expirable<T>>(initialCapacity);
		this.expireQueue = new PriorityQueue<T>(initialCapacity, expireComparator);
		this.timeoutSet = new HashSet<T>(initialCapacity);
	}

	public String getHost() {
		return host;
	}

	public Date getTime() {
		return new Date(lastTime.get());
	}

	public List<T> getTimeoutContexts() {
		List<T> l = new ArrayList<T>(timeoutQueue.size());
		for (Expirable<T> e : timeoutQueue) {
			l.add(e.item);
		}
		Collections.sort(l, timeoutComparator);
		return l;
	}

	public List<T> getExpireContexts() {
		List<T> l = new ArrayList<T>(expireQueue);
		Collections.sort(l, expireComparator);
		return l;
	}

	public int getTimeoutQueueLength() {
		return timeoutQueue.size();
	}

	public int getExpireQueueLength() {
		return expireQueue.size();
	}

	public void setTime(long now, boolean force) {
		if (force) {
			lastTime.set(now);
		} else {
			while (now > lastTime.get()) {
				long l = lastTime.get();
				if (lastTime.compareAndSet(l, now)) {
					evictContext(now);
					break;
				}
			}
		}
	}

	public void add(T item) {
		synchronized (expireQueue) {
			if (item.getExpireTime() != 0)
				expireQueue.add(item);
		}

		synchronized (timeoutQueue) {
			if (item.getTimeoutTime() != 0)
				addTimeout(item);
		}
	}

	public void updateTimeout(T item) {
		if (slog.isDebugEnabled()) {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			slog.debug("araqne logdb cep: update timeout [{}] of context [{}]", df.format(new Date(item.getTimeoutTime())),
					item.getKey());
		}

		synchronized (timeoutQueue) {
			if (item.getTimeoutTime() != 0 && !timeoutSet.contains(item)) {
				addTimeout(item);
			}
		}
	}

	// The ctx object will be added into the timeoutQueue again when ORIGINAL
	// timeout has met;
	// so following O(n) operation (remove) can be avoided.

	// synchronized (timeoutQueue) {
	// // reorder
	// timeoutQueue.remove(ctx);
	// timeoutQueue.add(ctx);
	// }

	private void addTimeout(T item) {
		timeoutQueue.add(new Expirable<T>(item, item.getTimeoutTime()));
		timeoutSet.add(item);
	}

	// This class caches ORIGINAL timeout time of EventClockItem.
	// It helps timeoutQueue always to be sorted by timeout time.
	// If the timeout time of an EventClockItem has updated,
	// EventClock attempts once to evict the EventClockItem by ORIGINAL timeout
	// time,
	// but add it again into the queue.
	private static class Expirable<T> implements Comparable<Expirable<T>> {
		private long expireTime;
		private T item;

		public Expirable(T item, long timeoutTime) {
			this.expireTime = timeoutTime;
			this.item = item;
		}

		public Expirable(T item) {
			this(item, -1);
		}

		public long getExpireTime() {
			return expireTime;
		}

		@Override
		public int compareTo(Expirable<T> t) {
			long d = this.expireTime - t.expireTime;
			return d < 0 ? -1 : (d > 0 ? +1 : 0);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((item == null) ? 0 : item.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			@SuppressWarnings("unchecked")
			Expirable<T> other = (Expirable<T>) obj;
			if (item == null) {
				if (other.item != null)
					return false;
			} else if (item != other.item)
				return false;
			return true;
		}

	}

	public void remove(T item) {
		synchronized (expireQueue) {
			expireQueue.remove(item);
		}

		synchronized (timeoutQueue) {
			timeoutQueue.remove(new Expirable<T>(item));
			timeoutSet.remove(item);
		}
	}

	private void evictContext(long now) {
		HashMap<EventKey, T> expiredEvictees = new HashMap<EventKey, T>();

		synchronized (expireQueue) {
			while (true) {
				T item = expireQueue.peek();
				if (item == null)
					break;

				if (item.getExpireTime() <= now) {
					expireQueue.poll();
					expiredEvictees.put(item.getKey(), item);
				} else
					break;
			}
		}

		for (T e : expiredEvictees.values())
			callback.onRemove(e, EventCause.EXPIRE);

		expiredEvictees = null;

		HashMap<EventKey, T> timeoutEvictees = new HashMap<EventKey, T>();

		synchronized (timeoutQueue) {
			while (true) {
				Expirable<T> e = timeoutQueue.peek();
				if (e == null)
					break;

				if (e.getExpireTime() <= now) {
					timeoutQueue.poll();
					timeoutSet.remove(e.item);
					// if timeout time has updated, don't evict and add again;
					if (e.item.getTimeoutTime() != 0 && e.item.getTimeoutTime() > e.getExpireTime()) {
						addTimeout(e.item);
					} else {
						timeoutEvictees.put(e.item.getKey(), e.item);
					}
				} else
					break;
			}
		}

		for (T e : timeoutEvictees.values())
			callback.onRemove(e, EventCause.TIMEOUT);
	}

	@Override
	public String toString() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		return host + " (timeout: " + timeoutQueue.size() + ", expire: " + expireQueue.size() + ") => "
				+ df.format(new Date(lastTime.get()));
	}

	private class TimeoutComparator implements Comparator<T> {
		@Override
		public int compare(T o1, T o2) {
			return (int) (o1.getTimeoutTime() - o2.getTimeoutTime());
		}
	}

	private class ExpireComparator implements Comparator<T> {
		@Override
		public int compare(T o1, T o2) {
			return (int) (o1.getExpireTime() - o2.getExpireTime());
		}
	}
}

// /*
// * Copyright 2014 Eediom Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
// package org.araqne.logdb.cep;
//
// import java.text.SimpleDateFormat;
// import java.util.*;
// import java.util.concurrent.atomic.AtomicLong;
//
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// public class EventClock {
// private final Logger slog = LoggerFactory.getLogger(EventClock.class);
//
// private final TimeoutComparator timeoutComparator = new TimeoutComparator();
// private final ExpireComparator expireComparator = new ExpireComparator();
// private final EventContextStorage storage;
// private final String host;
// private final PriorityQueue<Expirable> timeoutQueue;
// private final HashSet<EventContext> timeoutSet;
// private final PriorityQueue<EventContext> expireQueue;
//
// private AtomicLong lastTime = new AtomicLong();
//
// public EventClock(EventContextStorage storage, String host, long lastTime,
// int initialCapacity) {
// this.storage = storage;
// this.host = host;
// this.lastTime = new AtomicLong(lastTime);
// this.timeoutQueue = new PriorityQueue<Expirable>(initialCapacity);
// this.expireQueue = new PriorityQueue<EventContext>(initialCapacity,
// expireComparator);
// this.timeoutSet = new HashSet<EventContext>(initialCapacity);
// }
//
// public String getHost() {
// return host;
// }
//
// public Date getTime() {
// return new Date(lastTime.get());
// }
//
// public List<EventContext> getTimeoutContexts() {
// List<EventContext> l = new ArrayList<EventContext>(timeoutQueue.size());
// for (Expirable e : timeoutQueue) {
// l.add(e.ctx);
// }
// Collections.sort(l, timeoutComparator);
// return l;
// }
//
// public List<EventContext> getExpireContexts() {
// List<EventContext> l = Arrays.asList(expireQueue.toArray(new
// EventContext[0]));
// Collections.sort(l, expireComparator);
// return l;
// }
//
// public int getTimeoutQueueLength() {
// return timeoutQueue.size();
// }
//
// public int getExpireQueueLength() {
// return expireQueue.size();
// }
//
// public void setTime(long now, boolean force) {
// if (force) {
// lastTime.set(now);
// } else {
// while (now > lastTime.get()) {
// long l = lastTime.get();
// if (lastTime.compareAndSet(l, now)) {
// evictContext(now);
// break;
// }
// }
// }
// }
//
// public void add(EventContext ctx) {
// synchronized (expireQueue) {
// if (ctx.getExpireTime() != 0)
// expireQueue.add(ctx);
// }
//
// synchronized (timeoutQueue) {
// if (ctx.getTimeoutTime() != 0)
// addTimeout(ctx);
// }
// }
//
// public void updateTimeout(EventContext ctx) {
// if (slog.isDebugEnabled()) {
// SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
// slog.debug(
// "araqne logdb cep: update timeout [{}] of context [{}]",
// df.format(new Date(ctx.getTimeoutTime())),
// ctx.getKey());
// }
//
// synchronized (timeoutQueue) {
// if (ctx.getTimeoutTime() != 0 && !timeoutSet.contains(ctx)) {
// addTimeout(ctx);
// }
// }
//
// // The ctx object will be added into the timeoutQueue again when ORIGINAL
// timeout has met;
// // so following O(n) operation (remove) can be avoided.
//
// // synchronized (timeoutQueue) {
// // // reorder
// // timeoutQueue.remove(ctx);
// // timeoutQueue.add(ctx);
// // }
// }
//
// private void addTimeout(EventContext ctx) {
// timeoutQueue.add(new Expirable(ctx, ctx.getTimeoutTime()));
// timeoutSet.add(ctx);
// }
//
// // This class caches ORIGINAL timeout time of EventContext.
// // It helps timeoutQueue always to be sorted by timeout time.
// // If the timeout time of an EventContext has updated,
// // EventClock attempts once to evict the EventContext by ORIGINAL timeout
// time,
// // but add it again into the queue.
// private static class Expirable implements Comparable<Expirable> {
// private long expireTime;
// private EventContext ctx;
//
// public Expirable(EventContext ctx, long timeoutTime) {
// this.expireTime = timeoutTime;
// this.ctx = ctx;
// }
//
// public Expirable(EventContext ctx) {
// this.expireTime = -1;
// this.ctx = ctx;
// }
//
// public long getExpireTime() {
// return expireTime;
// }
//
// @Override
// public int compareTo(Expirable o) {
// long d = this.expireTime - o.expireTime;
// return d < 0 ? -1 : (d > 0 ? +1 : 0);
// }
//
// @Override
// public int hashCode() {
// final int prime = 31;
// int result = 1;
// result = prime * result + ((ctx == null) ? 0 : ctx.hashCode());
// return result;
// }
//
// @Override
// public boolean equals(Object obj) {
// if (this == obj)
// return true;
// if (obj == null)
// return false;
// if (getClass() != obj.getClass())
// return false;
// Expirable other = (Expirable) obj;
// if (ctx == null) {
// if (other.ctx != null)
// return false;
// } else if (ctx != other.ctx)
// return false;
// return true;
// }
//
// }
//
// public void remove(EventContext ctx) {
// synchronized (expireQueue) {
// expireQueue.remove(ctx);
// }
//
// synchronized (timeoutQueue) {
// timeoutQueue.remove(new Expirable(ctx));
// timeoutSet.remove(ctx);
// }
// }
//
// private void evictContext(long now) {
// HashMap<EventKey, EventContext> expiredEvictees = new HashMap<EventKey,
// EventContext>();
//
// synchronized (expireQueue) {
// while (true) {
// EventContext ctx = expireQueue.peek();
// if (ctx == null)
// break;
//
// if (ctx.getExpireTime() <= now) {
// expireQueue.poll();
// expiredEvictees.put(ctx.getKey(), ctx);
// } else
// break;
// }
// }
//
// for (Map.Entry<EventKey, EventContext> e : expiredEvictees.entrySet())
// storage.removeContext(e.getKey(), e.getValue(), EventCause.EXPIRE);
//
// expiredEvictees = null;
//
// HashMap<EventKey, EventContext> timeoutEvictees = new HashMap<EventKey,
// EventContext>();
//
// synchronized (timeoutQueue) {
// while (true) {
// Expirable e = timeoutQueue.peek();
// if (e == null)
// break;
//
// if (e.getExpireTime() <= now) {
// timeoutQueue.poll();
// timeoutSet.remove(e.ctx);
// // if timeout time has updated, don't evict and add again;
// if (e.ctx.getTimeoutTime() != 0 && e.ctx.getTimeoutTime() >
// e.getExpireTime()) {
// addTimeout(e.ctx);
// } else {
// timeoutEvictees.put(e.ctx.getKey(), e.ctx);
// }
// } else
// break;
// }
// }
//
// for (Map.Entry<EventKey, EventContext> e : timeoutEvictees.entrySet())
// storage.removeContext(e.getKey(), e.getValue(), EventCause.TIMEOUT);
// }
//
// @Override
// public String toString() {
// SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
// return host + " (timeout: " + timeoutQueue.size() + ", expire: " +
// expireQueue.size()
// + ") => "
// + df.format(new Date(lastTime.get()));
// }
//
// private static class TimeoutComparator implements Comparator<EventContext> {
// @Override
// public int compare(EventContext o1, EventContext o2) {
// return (int) (o1.getTimeoutTime() - o2.getTimeoutTime());
// }
// }
//
// private static class ExpireComparator implements Comparator<EventContext> {
// @Override
// public int compare(EventContext o1, EventContext o2) {
// return (int) (o1.getExpireTime() - o2.getExpireTime());
// }
// }
// }
