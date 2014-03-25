package org.araqne.logstorage.file;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.araqne.logstorage.LockKey;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogUtil;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class TableLockTest {

	LogStorage ls = mock(LogStorage.class);

	@BeforeClass
	public static void setup() {
	}

	@Test
	public void tableLockTest() {
		List<Log> logs = new ArrayList<Log>();

		logs.add(new Log("test1", LogUtil.getDay(new Date()), LogUtil.newLogData("line", "line0")));
		logs.add(new Log("test1", LogUtil.getDay(new Date()), LogUtil.newLogData("line", "line1")));
		logs.add(new Log("test1", LogUtil.getDay(new Date()), LogUtil.newLogData("line", "line2")));
		logs.add(new Log("test1", LogUtil.getDay(new Date()), LogUtil.newLogData("line", "line3")));
		logs.add(new Log("test1", LogUtil.getDay(new Date()), LogUtil.newLogData("line", "line4")));
		logs.add(new Log("test1", LogUtil.getDay(new Date()), LogUtil.newLogData("line", "line5")));
		logs.add(new Log("test1", LogUtil.getDay(new Date()), LogUtil.newLogData("line", "line6")));
		logs.add(new Log("test1", LogUtil.getDay(new Date()), LogUtil.newLogData("line", "line7")));
		logs.add(new Log("test1", LogUtil.getDay(new Date()), LogUtil.newLogData("line", "line8")));
		logs.add(new Log("test1", LogUtil.getDay(new Date()), LogUtil.newLogData("line", "line9")));
		
		assertTrue(ls.tryWrite(logs.get(0)));
		assertTrue(ls.tryWrite(logs.get(1)));
		
		ls.lock(new LockKey("test", "T1", null));
		
		assertFalse(ls.tryWrite(logs.get(2)));
		
		ls.unlock(new LockKey("test", "T1", null));
		
		assertTrue(ls.tryWrite(logs.get(2)));
	}
}
