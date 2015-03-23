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

public class EventClock {
	private final TimeoutComparator timeoutComparator = new TimeoutComparator();
	private final ExpireComparator expireComparator = new ExpireComparator();
	private final EventContextStorage storage;
	private final String host;
	private final PriorityQueue<EventContext> timeoutQueue;
	private final PriorityQueue<EventContext> expireQueue;

	private AtomicLong lastTime = new AtomicLong();

	public EventClock(EventContextStorage storage, String host, long lastTime, int initialCapacity) {
		this.storage = storage;
		this.host = host;
		this.lastTime = new AtomicLong(lastTime);
		this.timeoutQueue = new PriorityQueue<EventContext>(initialCapacity, timeoutComparator);
		this.expireQueue = new PriorityQueue<EventContext>(initialCapacity, expireComparator);
	}

	public String getHost() {
		return host;
	}

	public Date getTime() {
		return new Date(lastTime.get());
	}

	public List<EventContext> getTimeoutContexts() {
		List<EventContext> l = Arrays.asList(timeoutQueue.toArray(new EventContext[0]));
		Collections.sort(l, timeoutComparator);
		return l;
	}

	public List<EventContext> getExpireContexts() {
		List<EventContext> l = Arrays.asList(expireQueue.toArray(new EventContext[0]));
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

	public void add(EventContext ctx) {
		synchronized (expireQueue) {
			if (ctx.getExpireTime() != 0)
				expireQueue.add(ctx);
		}

		synchronized (timeoutQueue) {
			if (ctx.getTimeoutTime() != 0)
				timeoutQueue.add(ctx);
		}
	}

	public void updateTimeout(EventContext ctx) {
		synchronized (timeoutQueue) {
			// reorder
			timeoutQueue.remove(ctx);
			timeoutQueue.add(ctx);
		}
	}

	public void remove(EventContext ctx) {
		synchronized (expireQueue) {
			expireQueue.remove(ctx);
		}

		synchronized (timeoutQueue) {
			timeoutQueue.remove(ctx);
		}
	}

	private void evictContext(long now) {
		HashMap<EventKey, EventContext> expiredEvictees = new HashMap<EventKey, EventContext>();

		synchronized (expireQueue) {
			while (true) {
				EventContext ctx = expireQueue.peek();
				if (ctx == null)
					break;

				if (ctx.getExpireTime() <= now) {
					expireQueue.poll();
					expiredEvictees.put(ctx.getKey(), ctx);

				} else
					break;
			}
		}

		for (Map.Entry<EventKey, EventContext> e: expiredEvictees.entrySet())
			storage.removeContext(e.getKey(), e.getValue(), EventCause.EXPIRE);

		expiredEvictees = null;

		HashMap<EventKey, EventContext> timeoutEvictees = new HashMap<EventKey, EventContext>();

		synchronized (timeoutQueue) {
			while (true) {
				EventContext ctx = timeoutQueue.peek();
				if (ctx == null)
					break;

				if (ctx.getTimeoutTime() <= now) {
					timeoutQueue.poll();
					timeoutEvictees.put(ctx.getKey(), ctx);

				} else
					break;
			}
		}

		for (Map.Entry<EventKey, EventContext> e : timeoutEvictees.entrySet())
			storage.removeContext(e.getKey(), e.getValue(), EventCause.TIMEOUT);
	}

	@Override
	public String toString() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		return host + " (timeout: " + timeoutQueue.size() + ", expire: " + expireQueue.size() + ") => "
				+ df.format(new Date(lastTime.get()));
	}

	private static class TimeoutComparator implements Comparator<EventContext> {
		@Override
		public int compare(EventContext o1, EventContext o2) {
			return (int) (o1.getTimeoutTime() - o2.getTimeoutTime());
		}
	}

	private static class ExpireComparator implements Comparator<EventContext> {
		@Override
		public int compare(EventContext o1, EventContext o2) {
			return (int) (o1.getExpireTime() - o2.getExpireTime());
		}
	}
}
