package org.araqne.logdb.cep.offheap.timeout;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.araqne.logdb.cep.offheap.engine.StorageEngine;

public class OffHeapMapClock<K, V> {

	private final String host;
	private final StorageEngine<K, V> engine;
	//private final TimeoutUnitComparator comparator = new TimeoutUnitComparator();

	private AtomicLong lastTime = new AtomicLong();
	private TimeoutQueue timeoutQueue = new TimeoutQueue();
	private TimeoutQueue expireQueue = new TimeoutQueue();

	public OffHeapMapClock(StorageEngine<K, V> engine, String host, long lastTime) {
		this.engine = engine;
		this.host = host;
		this.lastTime = new AtomicLong(lastTime);
	}

	public OffHeapMapClock(StorageEngine<K, V> engine, String host) {
		this(engine, host, 0L);
	}

	// thread unsafe -> 상위 단계인 offheapmap에서 lock
	public void addExpireTime(TimeoutItem item) {
		expireQueue.add(item);
	}

	// thread unsafe -> 상위 단계인 offheapmap에서 lock
	public void addTimeoutTime(TimeoutItem item) {
		timeoutQueue.add(item);
	}

	public TimeoutQueue timeoutQueue() {
		return timeoutQueue;
	}

	public TimeoutQueue expireQueue() {
		return expireQueue;
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

	private void evictContext(long now) {
	//	HashMap<EventKey, TimeoutItem> expiredEvictees = new HashMap<EventKey, TimeoutItem>();
		
		synchronized (expireQueue) {
			while(true) {
				TimeoutItem item = expireQueue.peek();
				if(item == null)
					break;
				
				if(item.getTime() <= now) {
					expireQueue.remove();
					engine.evict(item);
					//expiredEvictees.put(item.getKey(), item);
				}else
					break;
			}
		}
	
		synchronized (timeoutQueue) {
			while(true) {
				TimeoutItem item = timeoutQueue.peek();
				if(item == null)
					break;
				
				if(item.getTime() <= now) {
					timeoutQueue.remove();
					engine.evict(item);
				} else 
					break;
			}
		}
	}

//	public void updateTimeout(TimeoutItem item) {
//		//item의 시간이 변경됨 => 필요없을듯?
//	}
	
	public Date getDate() {
		return new Date(lastTime.get());
	}

	public String getHost() {
		return host;
	}
	
	// @Override
	// public void setTime(long now, boolean force) {
	// throw new UnsupportedOperationException();
	// }
	// @Override
	// public String getHost() {
	// return host;
	// }
	//
	// @Override
	// public Date getTime() {
	// return new Date(lastTime.get());
	// }

	// XXX 필요한지 다시 확인
	public List<K> getTimeoutContexts() {
		List<K> l = new ArrayList<K>(timeoutQueue.size());
		for (int i = 0; i < timeoutQueue.size(); i++) {
			TimeoutItem item = timeoutQueue.get(i);
			K key = engine.loadKey(item.getAddress());
			l.add(key);
		}

		// Collections.sort(l, comparator);
		return l;
	}

	// XXX 필요한지 다시 확인
	public List<K> getExpireContexts() {
		List<K> l = new ArrayList<K>(expireQueue.size());

		for (int i = 0; i < expireQueue.size(); i++) {
			TimeoutItem item = expireQueue.get(i);
			K key = engine.loadKey(item.getAddress());
			l.add(key);
		}

		// Collections.sort(l, comparator);
		return l;
	}

	// @Override
	// public int getTimeoutQueueLength() {
	// return getTimeoutContexts().size();
	// }
	//
	// @Override
	// public int getExpireQueueLength() {
	// return getExpireContexts().size();
	// }
	//
	// @Override
	// public void setTime(long now, boolean force) {
	// throw new UnsupportedOperationException();
	// }
	//
	// @Override
	// public void add(K item) {
	// throw new UnsupportedOperationException();
	// }
	//
	// @Override
	// public void updateTimeout(K item) {
	// throw new UnsupportedOperationException();
	// }
	//
	// @Override
	// public void remove(K item) {
	// throw new UnsupportedOperationException();
	// }

	@Override
	public String toString() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		return host + " (timeout: " + timeoutQueue.size() + ", expire: " + expireQueue.size() + ") => "
				+ df.format(new Date(lastTime.get()));
	}

	// XXX 필요없을듯?
	private class TimeoutUnitComparator implements Comparator<TimeoutItem> {
		@Override
		public int compare(TimeoutItem o1, TimeoutItem o2) {
			long t1 = o1.getTime();
			long t2 = o2.getTime();

			if (t1 == t2)
				return 0;
			return t1 < t2 ? -1 : 1;
		}
	}

}
