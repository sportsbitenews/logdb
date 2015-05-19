package org.araqne.logparser.krsyslog.umv;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class ShellMonitorParserTest {
	@Test
	public void testDetectionLog() {
		String line = "Apr 17 12:50:24 192.168.0.71 20120417125030 : 1 : \"[VMWare_Test]\" : D : 10 : \"[khKim-PC]\" : 192.168.0.15 : 9999 : \"[미지정]\" :"
				+ "20120417124747 : 20120417125030 :   P  :	P	: \"[D:\\\\06.WebShell\\\\php\\\\C99Shell_v1.0 beta by SpyGrup.Org.php]\": 0   : 20080107030654  :	152950   :   20080107030654  :	152950   :   202  :	9488   :  12	:   \"[netstat -an ]\" : 1";
		Map<String, Object> m = parse(line);

		Object[] tokens = new Object[] { "1", "VMWare_Test", "D", "10", "khKim-PC", "192.168.0.15", "9999", "미지정",
				"20120417124747", "20120417125030", "P", "P", "D:\\06.WebShell\\php\\C99Shell_v1.0 beta by SpyGrup.Org.php", "0",
				"20080107030654", "152950", "20080107030654", "152950", "202", "9488", "12", "netstat -an ", "1" };

		int i = 0;
		for (String field : ShellMonitorParser.DETECTION_FIELDS)
			assertEquals(tokens[i++], m.get(field));
	}

	@Test
	public void testStatusLog1() {
		String line = "Apr 24 10:20:51 localhost 20120424102051	:   1  :	\"[TEST_Server]\"	:  S	:	A  :	23   :  \"[unknown]\"	:  192.168.0.16	:   2  :	\"[솔라리스]\":CT   :   1010  :	\"[[종료] 에이전트 접속]\"";
		Map<String, Object> m = parse(line);
		Object[] tokens = new Object[] { "1", "TEST_Server", "S", "A", "23", "unknown", "192.168.0.16", "2", "솔라리스", "CT",
				"1010", "[종료] 에이전트 접속" };

		int i = 0;
		for (String field : ShellMonitorParser.STATUS_FIELDS)
			assertEquals(tokens[i++], m.get(field));
	}

	@Test
	public void testStatusLog2() {
		String line = "Apr 24 10:20:57 localhost 20120424102057	:   1  :	\"[TEST_Server]\"	:  S	:	A  :	23   :  \"[unknown]\"	:  192.168.0.16	:   2  :	\"[솔라리스]\":DS   :   1049   :   \"[[시작] 에이전트 웹쉘탐지]\"";
		Map<String, Object> m = parse(line);
		Object[] tokens = new Object[] { "1", "TEST_Server", "S", "A", "23", "unknown", "192.168.0.16", "2", "솔라리스", "DS",
				"1049", "[시작] 에이전트 웹쉘탐지" };

		int i = 0;
		for (String field : ShellMonitorParser.STATUS_FIELDS)
			assertEquals(tokens[i++], m.get(field));
	}

	@Test
	public void testTransactionLog() {
		String line = "Oct   8 05:19:08 localhost 20121008051908:1:\"[TEST_Server]\":T:305:S:M:jkbang:\"[ jkbang]\":192.168.0.11:::S:1:\"[TEST_Server]\"::::1046:\"[서버 실행옵션 저장]\"";
		Map<String, Object> m = parse(line);

		Object[] tokens = new Object[] { "1", "TEST_Server", "T", "305", "S", "M", "jkbang", " jkbang", "192.168.0.11", null,
				null, "S", "1", "TEST_Server", null, null, null, "1046", "서버 실행옵션 저장" };

		int i = 0;
		for (String field : ShellMonitorParser.TRANSACTION_FIELDS)
			assertEquals(tokens[i++], m.get(field));
	}

	@Test
	public void testFilteringLog() {
		String line = "Oct   8 08:03:36 localhost 20121008080336	:   1  :	\"[TEST_Server]\"	:	F  :	305  :	\"[khkim-PC]\"	:	192.168.0.6 :  31  :  \"[Windows 2008]\"   :20121008080331   : 20121008080709   :  0	: \"[C:\\\\WebShell\\\\testdir\\\\copy_dir.bat]\" :	0   :  20121008080709	: 20120821133849  :196  :	20120821133849   :  196";
		Map<String, Object> m = parse(line);

		Object[] tokens = new Object[] { "1", "TEST_Server", "F", "305", "khkim-PC", "192.168.0.6", "31", "Windows 2008",
				"20121008080331", "20121008080709", "0", "C:\\WebShell\\testdir\\copy_dir.bat", "0", "20121008080709",
				"20120821133849", "196", "20120821133849", "196" };

		int i = 0;
		for (String field : ShellMonitorParser.FILTERING_FIELDS)
			assertEquals(tokens[i++], m.get(field));
	}

	@Test
	public void testAlertLog() {
		String line = "Oct   8 08:51:58 localhost 20121008085158	:   1  :	\"[TEST_Server]\"	:  A	:	305   :  \"[khkim-PC]\"	:  192.168.0.6	:  31:\"[Windows 2008]\"	:  20121008085158	:   246  :	WAS 설정 파일  변경	: [C:\\Windows\\System32\\inetsrv\\config\\applicationHost.config] ";
		Map<String, Object> m = parse(line);

		Object[] tokens = new Object[] { "1", "TEST_Server", "A", "305", "khkim-PC", "192.168.0.6", "31", "Windows 2008",
				"20121008085158", "246", "WAS 설정 파일  변경", "C:\\Windows\\System32\\inetsrv\\config\\applicationHost.config" };

		int i = 0;
		for (String field : ShellMonitorParser.ALERT_FIELDS)
			assertEquals(tokens[i++], m.get(field));
	}

	private Map<String, Object> parse(String line) {
		ShellMonitorParser p = new ShellMonitorParser();
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return p.parse(m);
	}
}
