//package org.araqne.logdb.cep;
//
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.Date;
//import java.util.List;
//import java.util.concurrent.atomic.AtomicLong;
//
//import org.araqne.logdb.cep.EventClock;
//import org.araqne.logdb.cep.EventClockCallback;
//import org.araqne.logdb.cep.EventClockItem;
//import org.araqne.logdb.cep.offheap.engine.Entry;
//import org.araqne.logdb.cep.offheap.engine.StorageEngine;
//
//public class OffHeapEventClock<K extends EventClockItem, V> extends EventClock<K> {
//	private static final int INITIAL_CAPACITY = 11;
//
//	private StorageEngine<K, V> engine;
//	// super 그대로 써도 되는지 확인?
//	private AtomicLong lastTime = new AtomicLong();
//	private String host;
//	private TimeoutQueue timeoutQueue;
//	private Un_ExpireQueue expireQueue;
//
//	// private final TimeoutComparator timeoutComparator = new
//	// TimeoutComparator();
//	// private final ExpireComparator expireComparator = new ExpireComparator();
//	private final TimeoutUnitComparator comparator = new TimeoutUnitComparator();
//
//	public OffHeapEventClock(StorageEngine<K, V> engine, EventClockCallback callback, String host, long lastTime,
//			int initialCapacity) {
//		super(callback, host, lastTime, initialCapacity);
//		this.engine = engine;
//		this.host = host;
//		this.lastTime = new AtomicLong(lastTime);
//	}
//
//	public OffHeapEventClock(StorageEngine<K, V> engine, String host, long lastTime) {
//		this(engine, null, host, lastTime, INITIAL_CAPACITY);
//	}
//
//	public OffHeapEventClock(StorageEngine<K, V> engine, String host) {
//		this(engine, null, host, new Date().getTime(), INITIAL_CAPACITY);
//	}
//
//	// thread unsafe -> 상위 단계인 offheapmap에서 lock
//	public void addExpireTime(TimeoutItem item) {
//		expireQueue.add(item);
//	}
//
//	// thread unsafe -> 상위 단계인 offheapmap에서 lock
//	public void addTimeoutTime(TimeoutItem item) {
//		timeoutQueue.add(item);
//	}
//
//	public TimeoutQueue timeoutQueue() {
//		return timeoutQueue;
//	}
//
//	public Un_ExpireQueue expireQueue() {
//		return expireQueue;
//	}
//
//	@Override
//	public String getHost() {
//		return host;
//	}
//
//	@Override
//	public Date getTime() {
//		return new Date(lastTime.get());
//	}
//
//	@Override
//	public List<K> getTimeoutContexts() {
//		List<K> l = new ArrayList<K>();
//		Collections.sort(l, comparator);
//		return l;
//	}
//
//	@Override
//	public List<K> getExpireContexts() {
//		List<K> l = new ArrayList<K>(expireQueue.size());
//
//		for (int i = 0; i < expireQueue.size(); i++) {
//			TimeoutItem item = expireQueue.get(i);
//			K key = engine.loadKey(item.getAddress());
//			l.add(key);
//		}
//
//		Collections.sort(l, comparator);
//		return l;
//	}
//
//	@Override
//	public int getTimeoutQueueLength() {
//		return getTimeoutContexts().size();
//	}
//
//	@Override
//	public int getExpireQueueLength() {
//		return getExpireContexts().size();
//	}
//
//	@Override
//	public void setTime(long now, boolean force) {
//		throw new UnsupportedOperationException();
//	}
//
//	@Override
//	public void add(K item) {
//		throw new UnsupportedOperationException();
//	}
//
//	@Override
//	public void updateTimeout(K item) {
//		throw new UnsupportedOperationException();
//	}
//
//	@Override
//	public void remove(K item) {
//		throw new UnsupportedOperationException();
//	}
//
//	@Override
//	public String toString() {
//		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//		return host + " (timeout: " + getTimeoutQueueLength() + ", expire: " + getExpireQueueLength() + ") => "
//				+ df.format(new Date(lastTime.get()));
//	}
//	
//	private class ExpireComparator implements Comparator<K> {
//
//		@Override
//		public int compare(K o1, K o2) {
//			return 0;
//		}
//		
//	}
//
//	private class TimeoutUnitComparator implements Comparator<TimeoutItem> {
//		@Override
//		public int compare(TimeoutItem o1, TimeoutItem o2) {
//			long t1 = o1.getDate();
//			long t2 = o2.getDate();
//
//			if (t1 == t2)
//				return 0;
//			return t1 < t2 ? -1 : 1;
//		}
//	}
//
//}
