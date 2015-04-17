package org.araqne.logparser.krsyslog.kyrgyzstan;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class ShellMonitorParserTest {
	@Test
	public void testSample() {
		String line = "Apr 17 12:50:24 192.168.0.71 20120417125030:1:\"[VMWare_Test]\":D:10:\"[khKim-PC]\":192.168.0.15:9999:\"[미지정]\":20120417124747:20120417125030:P:P:\"[D:\\06.WebShell\\php\\C99Shell_v1.0 beta by SpyGrup.Org.php]\":0:20080107030654:152950:20080107030654:152950:202:9488:12:\"[netstat -an ]\":1";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		ShellMonitorParser p = new ShellMonitorParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("Apr 17 12:50:24 192.168.0.71 20120417125030", m.get("syslog_server_time, send_address, management_server_time"));
		assertEquals("152950", m.get("file_size(current)"));
		assertEquals("[미지정]", m.get("group_name"));
		assertEquals("[D:\\06.WebShell\\php\\C99Shell_v1.0 beta by SpyGrup.Org.php]", m.get("file"));
	}
}
