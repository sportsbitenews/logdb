package org.araqne.logparser.krsyslog.citrix;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class NetScalerMpxParserTest {
	@Test
	public void testSslVpnTcpConnStat() {
		String line = "Mar  7 10:59:24 10.0.0.1  03/07/2013:01:59:45 GMT ns PPE-0 : SSLVPN TCPCONNSTAT 927685225 : Context uf48101_622e15d31819@1.2.3.4 - SessionId: 651173- User uf48101_622e15d31819 - Client_ip 1.2.3.4 - Nat_ip 211.193.193.102 - Vserver 13.14.15.16:443 - Source 1.2.3.4:32841 - Destination 5.6.7.8:20 - Start_time \"03/07/2013:01:59:45 GMT\" - End_time \"03/07/2013:01:59:45 GMT\" - Duration 00:00:00  - Total_bytes_send 1 - Total_bytes_recv 122881 - Total_compressedbytes_send 0 - Total_compressedbytes_recv 0 - Compression_ratio_send 0.00% - Compression_ratio_recv 0.00% - Access Allowed - Group(s) \"69B01E79DC24115918EF16E9E36A1B2\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		NetScalerMpxParser p = new NetScalerMpxParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("SSLVPN TCPCONNSTAT", m.get("event_type"));
		assertEquals("927685225", m.get("event_id"));
		assertEquals("uf48101_622e15d31819@1.2.3.4", m.get("context"));
		assertEquals("651173", m.get("sessionid"));
		assertEquals("uf48101_622e15d31819", m.get("user"));
		assertEquals("1.2.3.4", m.get("client_ip"));
		assertEquals("03/07/2013:01:59:45 GMT", m.get("end_time"));
		assertEquals("Allowed", m.get("access"));
		assertEquals("69B01E79DC24115918EF16E9E36A1B2", m.get("group_info"));
	}

	@Test
	public void testSslVpnHttpRequest() {
		String line = "Mar  7 10:59:45 1.1.1.1  03/07/2013:02:00:06 GMT ns PPE-0 : SSLVPN HTTPREQUEST 927687526 : Context user_1@9.10.11.12 - SessionId: 650592- url.com User user_1 : Group(s) 0F19CA7076048e2B37BB35524B49EFD,6B220752C39412e808A175FD0C1953B,90756D9463D4f79B3D0131475687BAF,B0CE79CA3CE430aA71AA42225CEA4E5 : Vserver 13.14.15.16:443 - 03/07/2013:02:00:06 GMT GET/hoho.js - -";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		NetScalerMpxParser p = new NetScalerMpxParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("SSLVPN HTTPREQUEST", m.get("event_type"));
		assertEquals("927687526", m.get("event_id"));
		assertEquals("user_1@9.10.11.12", m.get("context"));
		assertEquals("650592", m.get("sessionid"));
		assertEquals("url.com", m.get("url_info"));
		assertEquals("user_1", m.get("user"));
		assertEquals("13.14.15.16:443", m.get("vserver"));
		assertEquals("0F19CA7076048e2B37BB35524B49EFD,6B220752C39412e808A175FD0C1953B,90756D9463D4f79B3D0131475687BAF,B0CE79CA3CE430aA71AA42225CEA4E5", m.get("group_info"));
		assertEquals("03/07/2013:02:00:06 GMT", m.get("request_time"));
		assertEquals("GET/hoho.js", m.get("get_info"));
	}

	@Test
	public void testSslLogSslHandshakeSuccess() {
		String line = "Mar  7 11:00:22 2.2.2.2  03/07/2013:02:00:43 GMT ns PPE-0 : SSLLOG SSL_HANDSHAKE_SUCCESS 927690592 :  SPCBId 264444028 - ClientIP 1.2.3.4 - ClientPort 51646 - VserverServiceIP 13.14.15.16 - VserverServicePort 443 - ClientVersion TLSv1.0 - CipherSuite \"RC4-MD5 TLSv1 Non-Export 128-bit\" - Session Reuse";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		NetScalerMpxParser p = new NetScalerMpxParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("SSLLOG SSL_HANDSHAKE_SUCCESS", m.get("event_type"));
		assertEquals("927690592", m.get("event_id"));
		assertEquals("264444028", m.get("spcbid"));
		assertEquals("13.14.15.16", m.get("vserverserviceip"));
		assertEquals("RC4-MD5 TLSv1 Non-Export 128-bit", m.get("ciphersuite"));
		assertEquals("Reuse", m.get("session"));
	}

	@Test
	public void testSslVpnUdpFlowStat() {
		String line = "Mar  7 10:57:13 5.5.5.5  03/07/2013:01:57:34 GMT ns PPE-0 : SSLVPN UDPFLOWSTAT 927673685 : Context us81213_2311708c7e04@6.6.6.6 - SessionId: 651331- User us81213_2311708c7e04 - Client_ip 6.6.6.6 - Nat_ip 211.193.193.102 - Vserver 13.14.15.16:443 - Source 127.101.0.0:55955 - Destination 192.168.7.10:53 - Start_time \"03/07/2013:01:55:33 GMT\" - End_time \"03/07/2013:01:57:34 GMT\" - Duration 00:02:01  - Total_bytes_send 40 - Total_bytes_recv 88 - Access Allowed - Group(s) \"CE5E05127B342e8AA001B721E36D1C0,69B01E79DC24115918EF16E9E36A1B2\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		NetScalerMpxParser p = new NetScalerMpxParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("SSLVPN UDPFLOWSTAT", m.get("event_type"));
		assertEquals("927673685", m.get("event_id"));
		assertEquals("us81213_2311708c7e04@6.6.6.6", m.get("context"));
		assertEquals("651331", m.get("sessionid"));
		assertEquals("us81213_2311708c7e04", m.get("user"));
		assertEquals("6.6.6.6", m.get("client_ip"));
		assertEquals("03/07/2013:01:57:34 GMT", m.get("end_time"));
		assertEquals("Allowed", m.get("access"));
		assertEquals("CE5E05127B342e8AA001B721E36D1C0,69B01E79DC24115918EF16E9E36A1B2", m.get("group_info"));
	}

	@Test
	public void testSslVpnMessage() {
		String line = "Mar  7 11:00:55 8.8.8.8  03/07/2013:02:01:16 GMT ns PPE-0 : SSLVPN Message 927693187 :  \"sslvpn_add SSID 9f111, e, 27\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		NetScalerMpxParser p = new NetScalerMpxParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("SSLVPN Message", m.get("event_type"));
		assertEquals("927693187", m.get("event_id"));
		assertEquals("sslvpn_add SSID 9f111, e, 27", m.get("message_info"));
	}

	@Test
	public void testSslVpnTcpConnTimedOut() {
		String line = "Mar  7 11:01:31 9.9.9.9  03/07/2013:02:01:53 GMT ns PPE-0 : SSLVPN TCPCONN_TIMEDOUT 927696183 : Context a387568_525301f65890@6.6.6.6 - SessionId: 650290- User a387568_525301f65890 - Client_ip 6.6.6.6 - Nat_ip 211.193.193.102 - Vserver 13.14.15.16:443 - Last_contact \"03/07/2013:02:01:53 GMT\" - Group(s) \"0F19CA7076048e2B37BB35524B49EFD,AB256833CBD41a19DD6FBAA58E44EB5,AB73C9E13E3498d96C6390DAAA64BF2,B0CE79CA3CE430aA71AA42225CEA4E5\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		NetScalerMpxParser p = new NetScalerMpxParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("SSLVPN TCPCONN_TIMEDOUT", m.get("event_type"));
		assertEquals("927696183", m.get("event_id"));
		assertEquals("a387568_525301f65890@6.6.6.6", m.get("context"));
		assertEquals("650290", m.get("sessionid"));
		assertEquals("a387568_525301f65890", m.get("user"));
		assertEquals("6.6.6.6", m.get("client_ip"));
		assertEquals("03/07/2013:02:01:53 GMT", m.get("last_contact"));
		assertEquals("0F19CA7076048e2B37BB35524B49EFD,AB256833CBD41a19DD6FBAA58E44EB5,AB73C9E13E3498d96C6390DAAA64BF2,B0CE79CA3CE430aA71AA42225CEA4E5", m.get("group_info"));
	}

	@Test
	public void testSslVpnLogin() {
		String line = "Mar  7 11:02:14 10.0.0.1  03/07/2013:02:02:35 GMT ns PPE-0 : SSLVPN LOGIN 927699709 : Context uh12714_34fe88c9d64f@1.2.3.4 - SessionId: 651542- User uh12714_34fe88c9d64f - Client_ip 1.2.3.4 - Nat_ip \"Mapped Ip\" - Vserver 13.14.15.16:443 - Browser_type \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; AGEE 8.0;)\" - SSLVPN_client_type Agent - Group(s) \"F0B0188537B491dA58D3E3987E6165A,69B01E79DC24115918EF16E9E36A1B2\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		NetScalerMpxParser p = new NetScalerMpxParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("SSLVPN LOGIN", m.get("event_type"));
		assertEquals("927699709", m.get("event_id"));
		assertEquals("uh12714_34fe88c9d64f@1.2.3.4", m.get("context"));
		assertEquals("651542", m.get("sessionid"));
		assertEquals("uh12714_34fe88c9d64f", m.get("user"));
		assertEquals("1.2.3.4", m.get("client_ip"));
		assertEquals("Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; AGEE 8.0;)", m.get("browser_type"));
		assertEquals("Agent", m.get("sslvpn_client_type"));
		assertEquals("F0B0188537B491dA58D3E3987E6165A,69B01E79DC24115918EF16E9E36A1B2", m.get("group_info"));
	}

	@Test
	public void testSslVpnLogout() {
		String line = "Mar  7 14:11:55 10.0.0.1  03/07/2013:05:12:16 GMT ns PPE-0 : SSLVPN LOGOUT 928630814 : Context c3g2039_65ecd215305d@6.6.6.6 - SessionId: 650781- User c3g2039_65ecd215305d - Client_ip 6.6.6.6 - Nat_ip \"Mapped Ip\" - Vserver 13.14.15.16:443 - Start_time \"03/06/2013:23:24:07 GMT\" - End_time \"03/07/2013:05:12:16 GMT\" - Duration 05:48:09  - Http_resources_accessed 0 - NonHttp_services_accessed 0 - Total_TCP_connections 347 - Total_UDP_flows 0 - Total_policies_allowed 384 - Total_policies_denied 0 - Total_bytes_send 9326313 - Total_bytes_recv 179795866 - Total_compressedbytes_send 0 - Total_compressedbytes_recv 0 - Compression_ratio_send 0.00% - Compression_ratio_recv 0.00% - LogoutMethod \"Explicit\" - Group(s) \"4489C72CAFC4c0e9DF1614A226B1E1D,2E2214A9C4948a98ED7466A807D765A\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		NetScalerMpxParser p = new NetScalerMpxParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("SSLVPN LOGOUT", m.get("event_type"));
		assertEquals("928630814", m.get("event_id"));
		assertEquals("c3g2039_65ecd215305d@6.6.6.6", m.get("context"));
		assertEquals("650781", m.get("sessionid"));
		assertEquals("c3g2039_65ecd215305d", m.get("user"));
		assertEquals("6.6.6.6", m.get("client_ip"));
		assertEquals("03/07/2013:05:12:16 GMT", m.get("end_time"));
		assertEquals("05:48:09", m.get("duration"));
		assertEquals("0.00%", m.get("compression_ratio_send"));
		assertEquals("Explicit", m.get("logoutmethod"));
		assertEquals("4489C72CAFC4c0e9DF1614A226B1E1D,2E2214A9C4948a98ED7466A807D765A", m.get("group_info"));
	}

	@Test
	public void testAnotherFormatLogs() {
		String line = "07/18/2015:12:16:12 GMT ns 0-PPE-1 : SSLVPN TCPCONNSTAT 992720 0 : Context a487961_0629450b8636@110.0.0.2 - SessionId: 1983- User a487961_0629450b8636 - Client_ip 110.0.0.2 - Nat_ip 10.0.0.3 - Vserver 10.0.0.4:47873 - Source 10.0.0.5:58633 - Destination 10.0.0.6:443 - Start_time \"07/18/2015:12:16:09 GMT\" - End_time \"07/18/2015:12:16:12 GMT\" - Duration 00:00:03  - Total_bytes_send 2195 - Total_bytes_recv 9928 - Total_compressedbytes_send 0 - Total_compressedbytes_recv 0 - Compression_ratio_send 0.00% - Compression_ratio_recv 0.00% - Access Allowed - Group(s) \"D47CFBB27B94edfA279BFA24E19A809,0F19CA7076048e2B37BB35524B49EFD,B0CE79CA3CE430aA71AA42225CEA4E5\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		NetScalerMpxParser p = new NetScalerMpxParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("SSLVPN TCPCONNSTAT", m.get("event_type"));
		assertEquals("2195", m.get("total_bytes_send"));
		assertEquals("00:00:03", m.get("duration"));
		assertEquals("58633", m.get("source_port"));
	}

	@Test
	public void testAnotherFormatLogs2() {
		String line = "07/18/2015:12:16:04 GMT ns 0-PPE-0 : AAA EXTRACTED_GROUPS 979253 0 :  Extracted_groups \"0F19CA7076048e2B37BB35524B49EFD,D043BA1D8034ba198031185172C16EF\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		NetScalerMpxParser p = new NetScalerMpxParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("AAA EXTRACTED_GROUPS", m.get("event_type"));
	}

	@Test
	public void testAnotherFormatLogs3() {
		String line = "07/18/2015:12:16:11 GMT ns 0-PPE-0 : SNMP TRAP_SENT 979304 0 :  ipConflict (ipConflictAddr = 1.1.1.1, sysIpAddress = 2.2.2.2)";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		NetScalerMpxParser p = new NetScalerMpxParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("SNMP TRAP_SENT", m.get("event_type"));
	}
}
