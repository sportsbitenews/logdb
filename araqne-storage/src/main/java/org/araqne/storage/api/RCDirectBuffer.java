package org.araqne.storage.api;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class RCDirectBuffer {
	private AtomicInteger refCount = new AtomicInteger(0);
	private ByteBuffer buffer;
	private boolean destroyed = false;
	private String poolName;
	private String usageName;

	private boolean isDestroyed;

	private RCDirectBufferManager manager;

	public RCDirectBuffer(RCDirectBufferManager manager, ByteBuffer buffer, String poolName, String usageName) {
		this.manager = manager;
		this.buffer = buffer;
		this.poolName = poolName;
		this.usageName = usageName;
	}

	public ByteBuffer get() {
		return buffer;
	}

	private void destroy() {
		try {
			if (buffer == null)
				return;
			if (!buffer.isDirect())
				return;
			isDestroyed = true;
			manager.clean(buffer, poolName, usageName);
		} catch (Throwable t) {

		}
	}

	public RCDirectBuffer addRef() {
		synchronized (this) {
			if (!isDestroyed) {
				refCount.incrementAndGet();
				return this;
			} else {
				return null;
			}
		}
	}

	public RCDirectBuffer release() {
		synchronized (this) {
			int afterDec = refCount.decrementAndGet();
			if (afterDec == 0 && !destroyed) {
				destroy();
			}
		}
		return null;
	}

	public RCDirectBuffer asReadOnlyBuffer() {
		return new RCDirectBufferReadOnly(manager, buffer.asReadOnlyBuffer(), poolName, usageName);
	}

	public class RCDirectBufferReadOnly extends RCDirectBuffer {

		public RCDirectBufferReadOnly(RCDirectBufferManager manager, ByteBuffer buffer, String poolName, String usageName) {
			super(manager, buffer, poolName, usageName);
		}

		public RCDirectBuffer addRef() {
			throw new ReadOnlyRCDirectBufferException("addRef");
		}

		public RCDirectBuffer release() {
			throw new ReadOnlyRCDirectBufferException("release");
		}

		public class ReadOnlyRCDirectBufferException extends UnsupportedOperationException {
			private static final long serialVersionUID = 1L;
			private String operation;

			ReadOnlyRCDirectBufferException(String operation) {
				this.operation = operation;
			}

			@Override
			public String getMessage() {
				return "ReadOnly RCDirectBuffer does not support " + operation + " operation";
			}
		}

	}
}
