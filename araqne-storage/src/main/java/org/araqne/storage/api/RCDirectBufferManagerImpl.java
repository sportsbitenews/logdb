package org.araqne.storage.api;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "araqne-rcdirectbuffer-manager")
@Provides
public class RCDirectBufferManagerImpl implements RCDirectBufferManager {
	private Logger dbLogger = LoggerFactory.getLogger("A");
	private AtomicInteger objCount;
	private AtomicLong totalCapacity;

	private Method cleanerMethod = null;
	private Method cleanMethod = null;

	private ConcurrentHashMap<String, AtomicLong> poolSizes;
	private ConcurrentHashMap<String, Long> poolSizeLimits;
	private ConcurrentHashMap<String, AtomicLong> usageSizes;

	public static RCDirectBufferManagerImpl getTestManager() {
		RCDirectBufferManagerImpl manager = new RCDirectBufferManagerImpl();
		manager.start();
		manager.dbLogger.info("TEST RCDirectBufferManager HAS STARTED!!");
		return manager;
	}

	RCDirectBufferManagerImpl() {
	}

	public RCDirectBuffer allocateDirect(int capacity, String poolName, String usageName) throws ExceedPoolSizeLimitException {
		AtomicLong usageSize = usageSizes.get(usageName);
		if (usageSize == null) {
			usageSize = new AtomicLong(0);
			AtomicLong result = usageSizes.putIfAbsent(usageName, usageSize);
			if (result != null)
				usageSize = result;
		}

		usageSize.addAndGet(capacity);

		AtomicLong poolSize = poolSizes.get(poolName);
		if (poolSize == null) {
			poolSize = new AtomicLong(0);

			AtomicLong result = poolSizes.putIfAbsent(poolName, poolSize);
			if (result != null) {
				poolSize = result;
			}
		}

		Long poolSizeLimit = poolSizeLimits.get(poolName);
		if (poolSizeLimit == null) {
			poolSizeLimit = Long.MAX_VALUE;
		}

		Long futurePoolSize = poolSize.addAndGet(capacity);
		if (futurePoolSize > poolSizeLimit) {
			poolSize.addAndGet(-capacity);
			throw new ExceedPoolSizeLimitException(futurePoolSize + capacity, poolSizeLimit);
		}

		ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
		int objcnt = objCount.incrementAndGet();
		long totalCap = totalCapacity.addAndGet(buffer.capacity());
		if (dbLogger.isDebugEnabled())
			dbLogger.debug("directByteBuffer allocated: RCDirectBuffer objCount: {}, totalCapacity: {}", objcnt, totalCap);

		return new RCDirectBuffer(this, buffer, poolName, usageName);
	}

	public void setMemoryLimitOfPool(String poolName, Long size) {
		synchronized (this) {
			poolSizeLimits.put(poolName, size);
		}
	}

	public RCDirectBuffer allocateDirect(int capacity) {
		ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
		int objcnt = objCount.incrementAndGet();
		long totalCap = totalCapacity.addAndGet(buffer.capacity());
		if (dbLogger.isDebugEnabled())
			dbLogger.debug("directByteBuffer allocated: RCDirectBuffer objCount: {}, totalCapacity: {}", objcnt, totalCap);

		return new RCDirectBuffer(this, buffer, null, null);
	}

	@Validate
	public void start() {
		poolSizes = new ConcurrentHashMap<String, AtomicLong>();
		poolSizeLimits = new ConcurrentHashMap<String, Long>();
		usageSizes = new ConcurrentHashMap<String, AtomicLong>();

		objCount = new AtomicInteger(0);
		totalCapacity = new AtomicLong(0);
	}

	public long getTotalCapacity() {
		return totalCapacity.get();
	}

	public int getObjectCount() {
		return objCount.get();
	}

	public long getUsingPoolSize(String poolName) {
		return getLong(poolSizes.get(poolName));
	}

	public long getAvailablePoolSize(String poolName) {
		long using = getUsingPoolSize(poolName);
		Long limit = poolSizeLimits.get(poolName);
		if (limit == null) {
			limit = Long.MAX_VALUE;
		}

		return limit - using;
	}

	public long getUsingObjectSize(String usageName) {
		return getLong(usageSizes.get(usageName));
	}

	private long getLong(AtomicLong atomicLong) {
		if (atomicLong == null)
			return 0L;
		else
			return atomicLong.get();
	}

	public Iterable<String> getPoolNames() {
		return poolSizes.keySet();
	}

	public Iterable<String> getUsagesNames() {
		return usageSizes.keySet();
	}

	@Override
	public void clean(ByteBuffer buffer, String poolName, String usageName) {
		try {
			int capacity = buffer.capacity();
			if (cleanerMethod == null) {
				cleanerMethod = buffer.getClass().getMethod("cleaner");
				cleanerMethod.setAccessible(true);
			}
			Object cleaner = cleanerMethod.invoke(buffer);
			if (cleanMethod == null) {
				cleanMethod = cleaner.getClass().getMethod("clean");
				cleanMethod.setAccessible(true);
			}
			cleanMethod.invoke(cleaner);
			long totalCap = totalCapacity.addAndGet(-capacity);
			int objcnt = objCount.decrementAndGet();
			if (dbLogger.isDebugEnabled())
				dbLogger.debug("directByteBuffer destroyed: RCDirectBuffer objCount: {}, totalCapacity: {}",
						objcnt, totalCap);
		} catch (Throwable t) {
			dbLogger.warn("directByteBuffer destruction failed: ", t);
		} finally {
			long capacity = buffer.capacity();
			AtomicLong poolSize = poolSizes.get(poolName);
			poolSize.addAndGet(-capacity);

			if (poolSize.compareAndSet(0, 0))
				poolSizes.remove(poolName);

			AtomicLong usageSize = usageSizes.get(usageName);
			usageSize.addAndGet(-capacity);

			if (usageSize.compareAndSet(0, 0))
				usageSizes.remove(usageName);
		}

	}

	@Override
	public void clean(ByteBuffer buffer) {
		try {
			int capacity = buffer.capacity();
			if (cleanerMethod == null) {
				cleanerMethod = buffer.getClass().getMethod("cleaner");
				cleanerMethod.setAccessible(true);
			}
			Object cleaner = cleanerMethod.invoke(buffer);
			if (cleanMethod == null) {
				cleanMethod = cleaner.getClass().getMethod("clean");
				cleanMethod.setAccessible(true);
			}
			cleanMethod.invoke(cleaner);
			long totalCap = totalCapacity.addAndGet(-capacity);
			int objcnt = objCount.decrementAndGet();
			if (dbLogger.isDebugEnabled())
				dbLogger.debug("directByteBuffer destroyed: RCDirectBuffer objCount: {}, totalCapacity: {}",
						objcnt, totalCap);
		} catch (Throwable t) {
			dbLogger.warn("directByteBuffer destruction failed: ", t);
		}
	}

}
