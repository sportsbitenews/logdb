package org.araqne.logparser.krsyslog.secuwiz;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class SecuwizSslLogParserTest {

	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}

	@Test
	public void testParseloginLog() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String line = "Apr 16 06:28:03 182.161.130.3 Apr 16 06:27:40 localhost logger: admin001,172.16.0.2,login,10.1.1.100";
		Map<String, Object> m = new SecuwizSslLogParser().parse(line(line));

		assertNotNull(m);
		assertEquals("2015-04-16 06:28:03", sdf.format(m.get("sys_svr_time")));
		assertEquals("182.161.130.3", m.get("source_ip"));
		assertEquals("2015-04-16 06:27:40", sdf.format(m.get("vpn_time")));
		assertEquals("localhost", m.get("hostname"));
		assertEquals("logger", m.get("log_type"));
		assertEquals("admin001", m.get("connect_id"));
		assertEquals("172.16.0.2", m.get("user_virtual_ip"));
		assertEquals("login", m.get("login_out_tag"));
		assertEquals("10.1.1.100", m.get("user_real_ip"));
	}

	@Test
	public void testParseLogoutLog() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String line = "Apr 16 06:22:55 182.161.130.3 Apr 16 06:22:27 localhost logger: admin001,,logout,10.1.1.100";
		Map<String, Object> m = new SecuwizSslLogParser().parse(line(line));

		assertNotNull(m);
		assertEquals("2015-04-16 06:22:55", sdf.format(m.get("sys_svr_time")));
		assertEquals("182.161.130.3", m.get("source_ip"));
		assertEquals("2015-04-16 06:22:27", sdf.format(m.get("vpn_time")));
		assertEquals("localhost", m.get("hostname"));
		assertEquals("logger", m.get("log_type"));
		assertEquals("admin001", m.get("connect_id"));
		assertEquals(null, m.get("user_virtual_ip"));
		assertEquals("logout", m.get("login_out_tag"));
		assertEquals("10.1.1.100", m.get("user_real_ip"));
	}

	@Test
	public void testParseAccessLog() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String line = "Apr  8 06:57:33 172.16.0.2 Apr  8 06:57:45 SecuwaySSL access_log: serverIP=192.168.3.1,ID=admin001,port=30021,clientIP=10.1.2.100,natIP=172.16.0.2";
		Map<String, Object> m = new SecuwizSslLogParser().parse(line(line));

		assertNotNull(m);
		assertEquals("2015-04-08 06:57:33", sdf.format(m.get("sys_svr_time")));
		assertEquals("172.16.0.2", m.get("source_ip"));
		assertEquals("2015-04-08 06:57:45", sdf.format(m.get("vpn_time")));
		assertEquals("SecuwaySSL", m.get("hostname"));
		assertEquals("access_log", m.get("log_type"));

		assertEquals("192.168.3.1", m.get("server_ip"));
		assertEquals("admin001", m.get("id"));
		assertEquals("30021", m.get("port"));
		assertEquals("10.1.2.100", m.get("client_ip"));
		assertEquals("172.16.0.2", m.get("nat_ip"));
	}

	@Test
	public void testParseSystemLog() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String line = "May 16 06:57:33 172.16.0.1 May  8 14:27:53 SecuwaySSL system_log:server test";
		Map<String, Object> m = new SecuwizSslLogParser().parse(line(line));

		assertNotNull(m);
		assertEquals("2015-05-16 06:57:33", sdf.format(m.get("sys_svr_time")));
		assertEquals("172.16.0.1", m.get("source_ip"));
		assertEquals("2015-05-08 14:27:53", sdf.format(m.get("vpn_time")));
		assertEquals("SecuwaySSL", m.get("hostname"));
		assertEquals("system_log", m.get("log_type"));

		assertEquals("server test", m.get("message"));
	}
}
