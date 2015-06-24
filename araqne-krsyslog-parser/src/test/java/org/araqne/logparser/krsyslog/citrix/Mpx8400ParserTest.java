package org.araqne.logparser.krsyslog.citrix;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class Mpx8400ParserTest {
	@Test
	public void testLoginFailedLog() {
		String line = "Usermindori-Client_ip127.0.0.1Failure_reasonFAIL";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		Mpx8400Parser p = new Mpx8400Parser();
		Map<String, Object> m = p.parse(log);

		assertEquals("mindori", m.get("User"));
		assertEquals("127.0.0.1", m.get("Client_ip"));
		assertEquals("FAIL", m.get("Failure_reason"));
	}

	@Test
	public void testLoginLog() {
		String line = "Usermindori-Client_ip192.168.222.233-Nat_ip1.2.3.4-Vserverhoho:5050Browser_type\"chrome\"-Group(s)\"mindori_group\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		Mpx8400Parser p = new Mpx8400Parser();
		Map<String, Object> m = p.parse(log);

		assertEquals("mindori", m.get("User"));
		assertEquals("192.168.222.233", m.get("Client_ip"));
		assertEquals("1.2.3.4", m.get("Nat_ip"));
		assertEquals("hoho:5050", m.get("Vserver"));
		assertEquals("\"chrome\"", m.get("Browser_type"));
		assertEquals("\"mindori_group\"", m.get("Group(s)"));
	}

	@Test
	public void testLogoutLog() {
		String line = "Usermindori-Client_ip1.2.3.4Nat_ip5.6.7.8-Vserverhohoho:8080-Start_time\"10:00:00\"-End_time\"12:00:00\"-Duration10s-Http_resources_accessedtrue-Total_TCP_connections100-Total_policies_allowed50Total_policies_denied10-Total_bytes_send10000Total_bytes_recv20000-Total_compressedbytes_send100Total_compressedbytes_recv200-Compression_ratio_send5.2-Compression_ratio_recv6.7-LogoutMethod\"NORMAL\"-Group(s)\"mindori_group\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		Mpx8400Parser p = new Mpx8400Parser();
		Map<String, Object> m = p.parse(log);

		assertEquals("mindori", m.get("User"));
		assertEquals("1.2.3.4", m.get("Client_ip"));
		assertEquals("5.6.7.8", m.get("Nat_ip"));
		assertEquals("hohoho:8080", m.get("Vserver"));
		assertEquals("\"10:00:00\"", m.get("Start_time"));
		assertEquals("\"12:00:00\"", m.get("End_time"));
		assertEquals("10s", m.get("Duration"));
		assertEquals("true", m.get("Http_resources_accessed"));
		assertEquals("100", m.get("Total_TCP_connections"));
		assertEquals("50", m.get("Total_policies_allowed"));
		assertEquals("10", m.get("Total_policies_denied"));
		assertEquals("10000", m.get("Total_bytes_send"));
		assertEquals("20000", m.get("Total_bytes_recv"));
		assertEquals("100", m.get("Total_compressedbytes_send"));
		assertEquals("200", m.get("Total_compressedbytes_recv"));
		assertEquals("5.2", m.get("Compression_ratio_send"));
		assertEquals("6.7", m.get("Compression_ratio_recv"));
		assertEquals("\"NORMAL\"", m.get("LogoutMethod"));
		assertEquals("\"mindori_group\"", m.get("Group(s)"));
	}

}
