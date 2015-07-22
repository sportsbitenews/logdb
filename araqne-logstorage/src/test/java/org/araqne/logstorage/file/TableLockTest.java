package org.araqne.logstorage.file;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.araqne.logstorage.LockKey;
import org.araqne.logstorage.LockStatus;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogUtil;
import org.araqne.logstorage.TableLockImpl;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TableLockTest {

	static LogStorage ls = mock(LogStorage.class);
	static TableLockImpl lock;

	@BeforeClass
	public static void setup() throws InterruptedException {
		lock = new TableLockImpl(0);
		when(ls.lock(any(LockKey.class), any(String.class), any(Long.class), any(TimeUnit.class))).thenAnswer(
				new Answer<Boolean>() {
					@Override
					public Boolean answer(InvocationOnMock inv) throws Throwable {
						LockKey key = (LockKey) inv.getArguments()[0];
						String p = (String) inv.getArguments()[1];
						Lock writeLock = lock.writeLock(key.owner, p);
						return writeLock.tryLock();
					}
				});

		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock inv) throws Throwable {
				LockKey key = (LockKey) inv.getArguments()[0];
				String p = (String) inv.getArguments()[1];
				Lock writeLock = lock.writeLock(key.owner, p);
				writeLock.unlock();
				return null;
			}
		}).when(ls).unlock(any(LockKey.class), any(String.class));

		when(ls.lockStatus(any(LockKey.class))).thenAnswer(new Answer<LockStatus>() {
			@Override
			public LockStatus answer(InvocationOnMock inv) throws Throwable {
				@SuppressWarnings("unused")
				LockKey key = (LockKey) inv.getArguments()[0];
				return new LockStatus(
						lock.getOwner(), lock.availableShared(), lock.getReentrantCount(), lock.getPurposes());
			}
		});

		when(ls.tryWrite(any(Log.class))).thenAnswer(new Answer<Boolean>() {
			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				Lock readLock = lock.readLock();
				boolean locked = readLock.tryLock();
				if (locked)
					readLock.unlock();
				return locked;
			}
		});

	}

	@Test
	public void tableLockTest() throws InterruptedException {
		List<Log> logs = new ArrayList<Log>();

		logs.add(new Log("test1", LogUtil.getDay(new Date()), LogUtil.newLogData("line", "line0")));
		logs.add(new Log("test1", LogUtil.getDay(new Date()), LogUtil.newLogData("line", "line1")));
		logs.add(new Log("test1", LogUtil.getDay(new Date()), LogUtil.newLogData("line", "line2")));

		assertTrue(ls.tryWrite(logs.get(0)));
		assertTrue(ls.tryWrite(logs.get(1)));

		ls.lock(new LockKey("test", "T1", null), "test", Long.MAX_VALUE, TimeUnit.SECONDS);

		assertFalse(ls.tryWrite(logs.get(2)));
		assertEquals("test", ls.lockStatus(new LockKey("test", "T1", null)).getOwner());

		ls.unlock(new LockKey("test", "T1", null), "test");

		assertTrue(ls.tryWrite(logs.get(2)));

		assertEquals(null, ls.lockStatus(new LockKey("test", "T1", null)).getOwner());
	}

	@Test
	public void reentrantLockingTest() throws InterruptedException {
		// precondition
		LockKey lk = new LockKey("test", "T1", null);
		assertEquals(null, ls.lockStatus(lk).getOwner());

		// test
		assertTrue(ls.lock(lk, "test1", Long.MAX_VALUE, TimeUnit.SECONDS));
		assertTrue(ls.lock(lk, "test2", Long.MAX_VALUE, TimeUnit.SECONDS));
		assertFalse(ls.lock(new LockKey("test2", "T1", null), "test1", Long.MAX_VALUE, TimeUnit.SECONDS));
		assertEquals("test", ls.lockStatus(new LockKey("test", "T1", null)).getOwner());
		assertEquals(
				"[test1, test2]",
				Arrays.toString(ls.lockStatus(new LockKey("test", "T1", null)).getPurposes().toArray()));
		ls.unlock(lk, "test2");
		assertEquals("test", ls.lockStatus(new LockKey("test", "T1", null)).getOwner());
		assertFalse(ls.lock(new LockKey("test2", "T1", null), "test1", Long.MAX_VALUE, TimeUnit.SECONDS));
		ls.unlock(lk, "test1");

		// postcondition
		assertEquals(null, ls.lockStatus(new LockKey("test", "T1", null)).getOwner());
	}
}
