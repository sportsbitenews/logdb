package org.araqne.storage.api;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RCDirectBuffer {
	private static Logger dbLogger = LoggerFactory.getLogger("A");
	
	private static AtomicLong iid = new AtomicLong(0);
	private final long oid;
	private AtomicInteger refCount = new AtomicInteger(0);
	private ByteBuffer buffer;
	private boolean destroyed = false;
	private String poolName;
	private String usageName;

	private boolean isDestroyed;

	private RCDirectBufferManager manager;
	
	public RCDirectBuffer(RCDirectBufferManager manager, ByteBuffer buffer) {
		if (dbLogger.isDebugEnabled())
			this.oid = iid.incrementAndGet();
		else
			this.oid = 0;
		this.manager = manager;
		this.buffer = buffer;
	}

	public RCDirectBuffer(RCDirectBufferManager manager, ByteBuffer buffer, String poolName, String usageName) {
		if (dbLogger.isDebugEnabled())
			this.oid = iid.incrementAndGet();
		else
			this.oid = 0;
		this.manager = manager;
		this.buffer = buffer;
		this.poolName = poolName;
		this.usageName = usageName;
	}
	
	public RCDirectBuffer(RCDirectBufferManager manager, long oid, ByteBuffer buffer,
			String poolName, String usageName) {
		this.oid = oid;
		this.manager = manager;
		this.buffer = buffer;
		this.poolName = poolName;
		this.usageName = usageName;
	}

	public long getOID() {
		return oid;
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
			manager.clean(this, poolName, usageName);
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
		return new RCDirectBufferReadOnly(manager, this, poolName, usageName);
	}

	public class RCDirectBufferReadOnly extends RCDirectBuffer {

		public RCDirectBufferReadOnly(RCDirectBufferManager manager, RCDirectBuffer buffer, String poolName, String usageName) {
			super(manager, buffer.oid, buffer.get().asReadOnlyBuffer(), poolName, usageName);
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
