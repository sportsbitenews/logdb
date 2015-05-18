package org.araqne.storage.engine;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.storage.api.RCDirectBuffer;
import org.araqne.storage.api.RCDirectBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "araqne-rcdirectbuffer-manager")
@Provides
public class RCDirectBufferManagerImpl implements RCDirectBufferManager {
	private final Logger dbLogger = LoggerFactory.getLogger("direct-buffer-cleaner-logger");
	private AtomicInteger objCount;
	private AtomicLong totalCapacity;

	private Method cleanerMethod = null;
	private Method cleanMethod = null;

	public static RCDirectBufferManagerImpl getTestManager() {
		RCDirectBufferManagerImpl manager = new RCDirectBufferManagerImpl();
		manager.start();
		manager.dbLogger.info("TEST RCDirectBufferManager HAS STARTED!!");
		return manager;
	}

	RCDirectBufferManagerImpl() {
	}

	public RCDirectBuffer allocateDirect(int capacity) {
		ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
		int objcnt = objCount.incrementAndGet();
		long totalCap = totalCapacity.addAndGet(buffer.capacity());
		if (dbLogger.isDebugEnabled())
			dbLogger.debug("directByteBuffer allocated: RCDirectBuffer objCount: {}, totalCapacity: {}", objcnt, totalCap);
		return new RCDirectBuffer(this, buffer);
	}

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
				dbLogger.debug("directByteBuffer destroyed: RCDirectBuffer objCount: {}, totalCapacity: {}", objcnt, totalCap);
		} catch (Throwable t) {
			dbLogger.warn("directByteBuffer destruction failed: ", t);
		}
	}

	@Validate
	public void start() {
		objCount = new AtomicInteger(0);
		totalCapacity = new AtomicLong(0);
	}

	public long getTotalCapacity() {
		return totalCapacity.get();
	}

	public int getObjectCount() {
		return objCount.get();
	}
}
