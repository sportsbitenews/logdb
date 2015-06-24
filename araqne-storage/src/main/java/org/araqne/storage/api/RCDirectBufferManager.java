package org.araqne.storage.api;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface RCDirectBufferManager {
	public RCDirectBuffer allocateDirect(int capacity);

	public RCDirectBuffer allocateDirect(int capacity, String poolName, String usageName) throws ExceedPoolSizeLimitException;

	public long getTotalCapacity();

	public int getObjectCount();

	@Deprecated
	void clean(ByteBuffer buffer);

	void clean(ByteBuffer buffer, String poolName, String usageName);

	public void setMemoryLimitOfPool(String poolName, Long size);

	public long getUsingPoolSize(String poolName);

	public long getAvailablePoolSize(String poolName);

	public long getUsingObjectSize(String usageName);

	public Iterable<String> getPoolNames();

	public Iterable<String> getUsagesNames();

	public class ExceedPoolSizeLimitException extends IOException {
		private Long size;
		private Long limit;

		ExceedPoolSizeLimitException(Long size, Long limit) {
			this.size = size;
			this.limit = limit;
		}

		@Override
		public String getMessage() {
			return "ExceedPoolSizeLimitException : Allocated Offheap size is " + this.size + " but limit is " + this.limit;
		}
	}
}
