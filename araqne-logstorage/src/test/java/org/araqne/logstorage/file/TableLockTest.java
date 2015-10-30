package org.araqne.logstorage.file;

import static org.junit.Assert.*;

import static org.mockito.Matchers.any;

import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.araqne.logstorage.LockKey;
import org.araqne.logstorage.LockStatus;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogUtil;
import org.araqne.logstorage.TableLock;
import org.araqne.logstorage.TableLockImpl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TableLockTest {

	LogStorage ls;
	TableLockImpl lock;

	@Before
	public void setup() throws InterruptedException {
		ls = mock(LogStorage.class);
		lock = new TableLockImpl(0);
		when(ls.lock(any(LockKey.class), any(String.class), any(Long.class), any(TimeUnit.class))).thenAnswer(
				new Answer<UUID>() {
					@Override
					public UUID answer(InvocationOnMock inv) throws Throwable {
						LockKey key = (LockKey) inv.getArguments()[0];
						String p = (String) inv.getArguments()[1];
						TableLock writeLock = lock.writeLock(key.owner, p);
						return writeLock.tryLock();
					}
				});

		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock inv) throws Throwable {
				LockKey key = (LockKey) inv.getArguments()[0];
				String p = (String) inv.getArguments()[1];
				TableLock writeLock = lock.writeLock(key.owner, p);
				writeLock.unlock();
				return null;
			}
		}).when(ls).unlock(any(LockKey.class), any(String.class));

		when(ls.lockStatus(any(LockKey.class))).thenAnswer(new Answer<LockStatus>() {
			@Override
			public LockStatus answer(InvocationOnMock inv) throws Throwable {
				@SuppressWarnings("unused")
				LockKey key = (LockKey) inv.getArguments()[0];
				if (lock.getOwner() != null)
					return new LockStatus(
							lock.getOwner(), lock.availableShared(), lock.getReentrantCount(),
							lock.getPurposes());
				else
					return new LockStatus(lock.availableShared());
			}
		});

		when(ls.tryWrite(any(Log.class))).thenAnswer(new Answer<Boolean>() {
			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				TableLock readLock = lock.readLock();
				UUID locked = readLock.tryLock();
				if (locked != null)
					readLock.unlock();
				return locked != null;
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
		LockKey lk = new LockKey("test", "T2", null);
		assertEquals(null, ls.lockStatus(lk).getOwner());

		// test
		assertTrue(ls.lock(lk, "test1", Long.MAX_VALUE, TimeUnit.SECONDS) != null);
		assertTrue(ls.lock(lk, "test2", Long.MAX_VALUE, TimeUnit.SECONDS) != null);
		assertFalse(ls.lock(
				new LockKey("test2", "T2", null), "test1", Long.MAX_VALUE, TimeUnit.SECONDS) != null);
		assertEquals("test", ls.lockStatus(new LockKey("test", "T2", null)).getOwner());
		assertEquals(
				"[test2:1, test1:1]",
				sortedPurposes(ls.lockStatus(new LockKey("test", "T2", null)).getPurposes()));
		ls.unlock(lk, "test2");
		assertEquals("test", ls.lockStatus(new LockKey("test", "T2", null)).getOwner());
		assertFalse(ls.lock(
				new LockKey("test2", "T2", null), "test1", Long.MAX_VALUE, TimeUnit.SECONDS) != null);
		ls.unlock(lk, "test1");

		// postcondition
		assertEquals(null, ls.lockStatus(new LockKey("test", "T1", null)).getOwner());
	}

	private String sortedPurposes(Collection<String> purposes) {
		Object[] arr = purposes.toArray();
		Arrays.sort(arr, Collections.reverseOrder());
		return Arrays.toString(arr);
	}

	@Test
	public void ensureUnlockingTest() throws InterruptedException {
		LockKey lk = new LockKey("test", "T3", null);
		ls.lock(lk, "Live", Long.MAX_VALUE, TimeUnit.SECONDS);
		System.out.println(ls.lockStatus(lk));
		ls.unlock(lk, "Live");
		System.out.println(ls.lockStatus(lk));
		ls.unlock(lk, "Live");
		System.out.println(ls.lockStatus(lk));
		ls.lock(lk, "Sync", 0, TimeUnit.SECONDS);
		ls.unlock(lk, "Live");
		ls.lock(lk, "Sync", 0, TimeUnit.SECONDS);
		ls.lock(lk, "Live", 0, TimeUnit.SECONDS);
		ls.lock(lk, "Sync", 0, TimeUnit.SECONDS);
		ls.unlock(lk, "Live");
		System.out.println(ls.lockStatus(lk));
	}
}
