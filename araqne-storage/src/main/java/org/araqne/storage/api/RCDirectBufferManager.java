package org.araqne.storage.api;

import java.nio.ByteBuffer;

public interface RCDirectBufferManager {
	public RCDirectBuffer allocateDirect(int capacity);

	public long getTotalCapacity();

	public int getObjectCount();

	void clean(ByteBuffer buffer);
}
