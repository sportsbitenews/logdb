package org.araqne.logparser.krsyslog.cyberoam;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class CyberoamParserTest {
	@Test
	public void testSample() {
		String line = "<190>date=2015-02-05 time=13:26:56 timezone=\"K\tST\" device_name=\"C\\\"R15iNG\" device_id=C060891-DHOHO1 log_id=23 log_type=\"Firewall\" log_component=\"Firewall Rule\" log_subtype=\"Allowed\" status=\"Allow\" priority=Information duration=121 fw_rule_id=21 user_name=\"\" user_gp=\"\" iap=0 ips_policy_id=0 appfilter_policy_id=0 application=\"Secure Socket Layer Protocol\" in_interface=\"PortA\" out_interface=\"PortB\" src_mac=00: 0:00: 0:00: 0 src_ip=192.168.0.17 src_country_code= dst_ip=54.225.65.160 dst_country_code=USA protocol=\"TCP\" src_port=44393 dst_port=443 sent_pkts=6  recv_pkts=5 sent_bytes=436 recv_bytes=519 tran_src_ip=211.18.70.99 tran_src_port=0 tran_dst_ip= tran_dst_port=0 srczonetype=\"\" dstzonetype=\"\" dir_disp=\"\" connevent=\"Stop\" connid=\"4106971084\" vconnid=\"\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		CyberoamParser p = new CyberoamParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("2015-02-05", m.get("date"));
		assertEquals("C\\\"R15iNG", m.get("device_name"));
		assertEquals("Firewall Rule", m.get("log_component"));
		assertEquals("00: 0:00: 0:00: 0", m.get("src_mac"));
		assertEquals("", m.get("srczonetype"));
		assertEquals("", m.get("vconnid"));
	}
	
	@Test
	public void testSample2() {
		String line = "<190>date=2015-02-05 time=13:26:56 timezone=\"K\tST\" device_name=\"C\\\"R15iNG\" device_id=C02110891-WER log_id=45 log_type=\"Firewall\" log_component=\"Firewall Rule\" log_subtype=\"Allowed\" status=\"Allow\" priority=Information duration=121 fw_rule_id=21 user_name=\"\" user_gp=\"\" iap=0 ips_policy_id=0 appfilter_policy_id=0 application=\"Secure Socket Layer Protocol\" in_interface=\"PortA\" out_interface=\"PortB\" src_mac=00: 0:00: 0:00: 0 src_ip=192.168.110.17 src_country_code= dst_ip=54.225.215.160 dst_country_code=USA protocol=\"TCP\" src_port=44393 dst_port=443 sent_pkts=6  recv_pkts=5 sent_bytes=436 recv_bytes=519 tran_src_ip=211.108.70.99 tran_src_port=0 tran_dst_ip= tran_dst_port=0 srczonetype=\"\" dstzonetype=\"\" dir_disp=\"\" connevent=\"Stop\" connid=\"4106971084\" vconnid=3756";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);
		
		CyberoamParser p = new CyberoamParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("2015-02-05", m.get("date"));
		assertEquals("C\\\"R15iNG", m.get("device_name"));
		assertEquals("Firewall Rule", m.get("log_component"));
		assertEquals("00: 0:00: 0:00: 0", m.get("src_mac"));
		assertEquals("", m.get("srczonetype"));
		assertEquals("3756", m.get("vconnid"));
	}

	@Test
	public void testSample3() {
		String line = "date=2015-05-07 time=17:35:48 timezone=\"KST\" device_name=\"CR50ia\" device_id=C0994023084-HHHQ log_id=12222 log_type=\"Firewall\" log_component=\"Appliance Access\" log_subtype=\"Denied\" status=\"Deny\" priority=Information duration=0 fw_rule_id=0 user_name=\"\" user_gp=\"\" iap=0 ips_policy_id=0 appfilter_policy_id=0 application=\"\" in_interface=\"PortB\" out_interface=\"\" src_mac=00:ze:67:8a:65:a4 src_ip=255.255.255.48 src_country_code= dst_ip=213.222.255.255 dst_country_code= protocol=\"UDP\" src_port=137 dst_port=137 sent_pkts=0 recv_pkts=550 sent_bytes=0 recv_bytes=0 tran_src_ip= tran_src_port=0 tran_dst_ip= tran_dst_port=0 srczonetype=\"\" dstzonetype=\"\" dir_disp=\"\" connid=\"\" vconnid=\"\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		CyberoamParser p = new CyberoamParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("550", m.get("recv_pkts"));
	}
}
