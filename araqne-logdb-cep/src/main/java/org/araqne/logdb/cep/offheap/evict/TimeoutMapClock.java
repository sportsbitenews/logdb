package org.araqne.logdb.cep.offheap.evict;

import java.io.Closeable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.araqne.logdb.cep.offheap.engine.Storage;

public class TimeoutMapClock<K, V> implements Closeable {
	private final String host;
	private final Storage<K, V> engine;

	private AtomicLong lastTime = new AtomicLong();
	private EvictQueue timeoutQueue = new EvictQueue();
	private EvictQueue expireQueue = new EvictQueue();

	public TimeoutMapClock(Storage<K, V> engine, String host, long lastTime) {
		this.engine = engine;
		this.host = host;
		this.lastTime = new AtomicLong(lastTime);
	}

	public TimeoutMapClock(Storage<K, V> engine, String host) {
		this(engine, host, 0L);
	}

	public void addExpireTime(EvictItem item) {
		expireQueue.add(item);
	}

	public void addTimeoutTime(EvictItem item) {
		timeoutQueue.add(item);
	}

	public EvictQueue timeoutQueue() {
		return timeoutQueue;
	}

	public EvictQueue expireQueue() {
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
		synchronized (expireQueue) {
			while(true) {
				EvictItem item = expireQueue.peek();
				if(item == null)
					break;
				
				if(item.getTime() <= now) {
					expireQueue.remove();
					engine.evict(item);
				}else
					break;
			}
		}
	
		synchronized (timeoutQueue) {
			while(true) {
				EvictItem item = timeoutQueue.peek();
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

	public Date getDate() {
		return new Date(lastTime.get());
	}

	public String getHost() {
		return host;
	}
	
	public void clear() {
		timeoutQueue.clear();
		expireQueue.clear();
	}
	
	public void close() {
		safeClose(timeoutQueue);
		safeClose(expireQueue);
	}
	
	private void safeClose(Closeable c) {
		if (c == null)
			return;

		try {
			c.close();
		} catch (Throwable e) {
		}
	}

	@Override
	public String toString() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		return host + " (timeout: " + timeoutQueue.size() + ", expire: " + expireQueue.size() + ") => "
				+ df.format(new Date(lastTime.get()));
	}

}
