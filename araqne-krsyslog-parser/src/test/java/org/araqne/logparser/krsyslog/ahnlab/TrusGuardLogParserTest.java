/*
 * Copyright 2012 Future Systems
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logparser.krsyslog.ahnlab;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

public class TrusGuardLogParserTest {

	@Test
	public void testCncDetectionLogV3() {
		String line = "3`0`2`1`100000`6060`20140116`12:30:10`0``1.1.1.1`123`2.2.2.2`321`CNC탐지`profile1`2`1`0`3`0`1`1`3`0`Malware`Trojan/Win32.Scar`기관코드`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("100000", m.get("utm_id"));

		assertEquals(6060, m.get("module_flag"));
		assertEquals(0, m.get("severity"));
		assertEquals(null, m.get("protocol"));
		assertEquals("1.1.1.1", m.get("src_ip"));
		assertEquals(123, m.get("src_port"));
		assertEquals("2.2.2.2", m.get("dst_ip"));
		assertEquals(321, m.get("dst_port"));
		assertEquals("CNC탐지", m.get("module_name"));
		assertEquals("profile1", m.get("profile_name"));
		assertEquals("보통", m.get("risk_score_db"));
		assertEquals("낮음", m.get("risk_score_user"));
		assertEquals("허용", m.get("risk_action_user"));
		assertEquals("높음", m.get("diffusion_score_db"));
		assertEquals("사용안함", m.get("diffusion_score_user"));
		assertEquals("차단", m.get("diffusion_action_user"));
		assertEquals("낮음", m.get("accuracy_score_db"));
		assertEquals("높음", m.get("accuracy_score_user"));
		assertEquals("허용", m.get("accuracy_action_user"));
		assertEquals("Malware", m.get("botnet_name"));
		assertEquals("Trojan/Win32.Scar", m.get("diag_name"));
		assertEquals("기관코드", m.get("inst_code"));
	}

	@Test
	public void testSystemIsolationLogV3() {
		String line = "3`0`2`1`100000`6050`20071001`16:55:10`0``1.1.1.1`123`2.2.2.2`321`0``IPS`시스템을격리 해제했습니다.`기관코드`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("100000", m.get("utm_id"));

		assertEquals(6050, m.get("module_flag"));
		assertEquals(0, m.get("severity"));
		assertEquals(null, m.get("protocol"));
		assertEquals("1.1.1.1", m.get("src_ip"));
		assertEquals(123, m.get("src_port"));
		assertEquals("2.2.2.2", m.get("dst_ip"));
		assertEquals(321, m.get("dst_port"));
		assertEquals("격리", m.get("action"));
		assertEquals(null, m.get("user"));
		assertEquals("IPS", m.get("module_name"));
		assertEquals("시스템을격리 해제했습니다.", m.get("desc"));
		assertEquals("기관코드", m.get("inst_code"));
	}

	@Test
	public void testIpxQosLBQoSLogV3() {
		String line = "3`0`1`1`100000`6041`20080328`01:57:51`4`17`192.168.1.1`4993`211.41.4.33`13568`3007``대용량 웹 트래픽`Apply 제한QoS`1234567`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(1, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("100000", m.get("utm_id"));

		// check log data
		assertEquals(6041, m.get("module_flag"));
		assertEquals(4, m.get("severity"));
		assertEquals("17", m.get("protocol"));
		assertEquals("192.168.1.1", m.get("src_ip"));
		assertEquals(4993, m.get("src_port"));
		assertEquals("211.41.4.33", m.get("dst_ip"));
		assertEquals(13568, m.get("dst_port"));
		assertEquals("3007", m.get("action"));
		assertEquals(null, m.get("user"));
		assertEquals("대용량 웹 트래픽", m.get("module_name"));
		assertEquals("Apply 제한QoS", m.get("description"));
		assertEquals("1234567", m.get("inst_code"));
	}

	@Test
	public void testIpxQosLogV3() {
		String line = "3`0`2`1`100000`6040`20100526`12:46:35`0``````3009``QoS 모니터`100K`eth2`64000`1000` 기관코드`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("100000", m.get("utm_id"));

		assertEquals(6040, m.get("module_flag"));
		assertEquals(0, m.get("severity"));
		assertEquals(null, m.get("protocol"));
		assertEquals(null, m.get("src_ip"));
		assertEquals(null, m.get("src_port"));
		assertEquals(null, m.get("dst_ip"));
		assertEquals(null, m.get("dst_port"));
		assertEquals("3009", m.get("action"));
		assertEquals(null, m.get("user"));
		assertEquals("QoS 모니터", m.get("module_name"));
		assertEquals("100K", m.get("qos_name"));
		assertEquals("eth2", m.get("eth_name"));
		assertEquals(64000L, m.get("bps"));
		assertEquals(1000L, m.get("pps"));
		assertEquals(" 기관코드", m.get("inst_code"));
	}

	@Test
	public void testIpxApplicationControlLogV3() {
		String line = "3`0`2`1`100000`6031`20071025`09:16:38`3`6`172.16.108.144`3204`121.140.211.81`9101`3003`user`module_name`profile1`group1`IDS_Social``desc`1133`9000`기관코드`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("100000", m.get("utm_id"));

		assertEquals(6031, m.get("module_flag"));
		assertEquals(3, m.get("severity"));
		assertEquals("6", m.get("protocol"));
		assertEquals("172.16.108.144", m.get("src_ip"));
		assertEquals(3204, m.get("src_port"));
		assertEquals("121.140.211.81", m.get("dst_ip"));
		assertEquals(9101, m.get("dst_port"));
		assertEquals("허용", m.get("action"));
		assertEquals("user", m.get("user"));
		assertEquals("module_name", m.get("module_name"));
		assertEquals("profile1", m.get("profile_name"));
		assertEquals("group1", m.get("group_name"));
		assertEquals("IDS_Social", m.get("app_name"));
		assertEquals(null, m.get("alarm_type"));
		assertEquals("desc", m.get("desc"));
		assertEquals(1133L, m.get("rule_id"));
		assertEquals(9000L, m.get("log_id"));
		assertEquals("기관코드", m.get("inst_code"));
	}

	@Test
	public void testIpxIPSLogV3() {
		String line = "3`0`2`1`100000`6030`20070515`15:45:41`2`17`5.5.5.1`10409`4.4.4.5`31335`3001``IPS`2012`eth0`0800`00:03:47:B5:B0:7`10231`65535`DDOS Trin00 Daemon to Mastermessage detected`기관코드`eth1`ICES`Zone1`1001`Profile1`Group1`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("100000", m.get("utm_id"));

		assertEquals(6030, m.get("module_flag"));
		assertEquals(2, m.get("severity"));
		assertEquals("17", m.get("protocol"));
		assertEquals("5.5.5.1", m.get("src_ip"));
		assertEquals(10409, m.get("src_port"));
		assertEquals("4.4.4.5", m.get("dst_ip"));
		assertEquals(31335, m.get("dst_port"));
		assertEquals("차단", m.get("action"));
		assertEquals(null, m.get("user"));
		assertEquals("IPS", m.get("module_name"));
		assertEquals("2012", m.get("reason"));
		assertEquals("eth0", m.get("rx_nif"));
		assertEquals("0800", m.get("eth_protocol"));
		assertEquals("00:03:47:B5:B0:7", m.get("src_mac"));
		assertEquals(10231L, m.get("rule_id"));
		assertEquals("65535", m.get("vlan_id"));

		assertEquals("DDOS Trin00 Daemon to Mastermessage detected", m.get("message"));
		assertEquals("기관코드", m.get("inst_code"));
		assertEquals("eth1", m.get("tx_nif"));
		assertEquals("ICES", m.get("network_direction"));
		assertEquals("Zone1", m.get("network_id"));
		assertEquals(1001L, m.get("log_id"));
		assertEquals("Profile1", m.get("profile_name"));
		assertEquals("Group1", m.get("group_name"));

	}

	@Test
	public void testIpxFirewallLogV3() {
		String line = "3`0`1`1`100000`6020`20071025`17:46:26`3`6`UTM_ADMINHOST`172.16.108.152`4430`172.16.108.211`50005`eth0`unknown````1021`8`724`7``31`2`기관코드`1`DMZ`INTERNAL`111111`222`4````";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(1, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("100000", m.get("utm_id"));

		assertEquals(6020, m.get("module_flag"));
		assertEquals("Expire", m.get("logtype"));
		assertEquals("6", m.get("protocol"));
		assertEquals("UTM_ADMINHOST", m.get("policy_id"));
		assertEquals("172.16.108.152", m.get("src_ip"));
		assertEquals(4430, m.get("src_port"));
		assertEquals("172.16.108.211", m.get("dst_ip"));
		assertEquals(50005, m.get("dst_port"));
		assertEquals("eth0", m.get("in_nic"));
		assertEquals("unknown", m.get("out_nic"));
		assertEquals(null, m.get("snat_type"));
		assertEquals(null, m.get("snat_ip"));
		assertEquals(null, m.get("snat_port"));
		assertEquals(1021L, m.get("sent_data"));
		assertEquals(8L, m.get("sent_pkt"));
		assertEquals(724L, m.get("rcvd_data"));
		assertEquals(7L, m.get("rcvd_pkt"));
		assertEquals(null, m.get("duration"));
		assertEquals("31", m.get("state"));
		assertEquals("2", m.get("reason"));
		assertEquals("기관코드", m.get("inst_code"));
		assertEquals("1", m.get("tcp_flag"));
		assertEquals("DMZ", m.get("in_zone"));
		assertEquals("INTERNAL", m.get("out_zone"));
		assertEquals(111111L, m.get("rule_id"));
		assertEquals("222", m.get("nat_id"));
		assertEquals(4, m.get("ip_ver"));
		assertEquals(null, m.get("dnat_type"));
		assertEquals(null, m.get("dnat_ip"));
		assertEquals(null, m.get("dnat_port"));
	}

	@Test
	public void testManagementSystemStatusLogV3() {
		String line = "3`0`2`1`100000`6011`20100704`12:46:35`Status로그`2`22`30`8`378000`224000`53`19`OFF`기관코드`100`50`1234000`34020`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("100000", m.get("utm_id"));

		// check log data
		assertEquals(6011, m.get("module_flag"));
		assertEquals("Status로그", m.get("module_name"));
		assertEquals(2, m.get("cpu"));
		assertEquals(22, m.get("mem"));
		assertEquals(30, m.get("hdd"));
		assertEquals(8, m.get("session"));
		assertEquals(378000L, m.get("in_data"));
		assertEquals(224000L, m.get("out_data"));
		assertEquals(53L, m.get("in_pkt"));
		assertEquals(19L, m.get("out_pkt"));
		assertEquals("OFF", m.get("ha"));

		assertEquals("기관코드", m.get("inst_code"));
		assertEquals(100L, m.get("allow_pps"));
		assertEquals(50L, m.get("deny_pps"));
		assertEquals(1234000L, m.get("allow_bps"));
		assertEquals(34020L, m.get("deny_bps"));
	}

	@Test
	public void testManagementSystemNewLogV3() {
		String line = "3`0`2`1`100000`6010`20071026`12:48:08`5``````0`user1`운영 로그`TrusGuard UTM의정책을 적용했습니다.`기관코드`1`1222`1`10`1.1.1.1`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("100000", m.get("utm_id"));

		// check log data
		assertEquals(6010, m.get("module_flag"));
		assertEquals(5, m.get("severity"));
		assertEquals(null, m.get("protocol"));
		assertEquals(null, m.get("src_ip"));
		assertEquals(null, m.get("src_port"));
		assertEquals(null, m.get("dst_ip"));
		assertEquals(null, m.get("dst_port"));
		assertEquals("격리", m.get("action"));
		assertEquals("user1", m.get("user"));
		assertEquals("운영 로그", m.get("module_name"));
		assertEquals("TrusGuard UTM의정책을 적용했습니다.", m.get("description"));
		assertEquals("기관코드", m.get("inst_code"));
		assertEquals("일반관리자", m.get("user_type"));
		assertEquals(1222, m.get("alert_id"));
		assertEquals(1, m.get("log_type"));
		assertEquals(10, m.get("log_sub_type"));
		assertEquals("1.1.1.1", m.get("user_addr"));
	}

	@Test
	public void testManagementSystemOldLogV3() {
		String line = "3`0`2`1`100000`6010`20071026`12:48:08`5``````0`user1`운영 로그`TrusGuard UTM의정책을 적용했습니다.`기관코드`1`1222`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("100000", m.get("utm_id"));

		// check log data
		assertEquals(6010, m.get("module_flag"));
		assertEquals(5, m.get("severity"));
		assertEquals(null, m.get("protocol"));
		assertEquals(null, m.get("src_ip"));
		assertEquals(null, m.get("src_port"));
		assertEquals(null, m.get("dst_ip"));
		assertEquals(null, m.get("dst_port"));
		assertEquals("격리", m.get("action"));
		assertEquals("user1", m.get("user"));
		assertEquals("운영 로그", m.get("module_name"));
		assertEquals("TrusGuard UTM의정책을 적용했습니다.", m.get("description"));
		assertEquals("기관코드", m.get("inst_code"));
		assertEquals("일반관리자", m.get("user_type"));
		assertEquals(1222, m.get("alert_id"));
		assertEquals(null, m.get("log_type"));
		assertEquals(null, m.get("log_sub_type"));
		assertEquals(null, m.get("user_addr"));
	}

	@Test
	public void testFilteringExceptionsFilterAllowLogV3() {
		String line = "3`0`2`1`000000`3181`20090911`18:45:52`10540`6`172.16.32.34`192.168.16.15`80`1234`16000`zone0`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		assertEquals(3181, m.get("module_flag"));
		assertEquals("10540", m.get("duration"));
		assertEquals("6", m.get("protocol"));
		assertEquals("172.16.32.34", m.get("src_ip"));
		assertEquals("192.168.16.15", m.get("dst_ip"));
		assertEquals(80, m.get("dst_port"));
		assertEquals(1234L, m.get("allow_packets"));
		assertEquals(16000L, m.get("allow_bytes"));
		assertEquals("zone0", m.get("zone_name"));
	}

	@Test
	public void testAttackLogV3() {
		String line = "3`0`2`1`000000`3171`20090911`18:45:52`1800000``11364f4a-aa0e-42bc-92ea-0fe6a8e01744`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		assertEquals(3171, m.get("module_flag"));
		assertEquals("1800000", m.get("duration"));
		assertEquals(null, m.get("zone_name"));
		assertEquals("11364f4a-aa0e-42bc-92ea-0fe6a8e01744", m.get("attack_id"));
	}

	@Test
	public void testSegmentProtectionFilterStatusLogV3() {
		String line = "3`0`2`1`000000`3160`20090911`18:45:52`10540`912.168.10.0`24`64`zone`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		assertEquals(3160, m.get("module_flag"));
		assertEquals("10540", m.get("duration"));
		assertEquals("912.168.10.0", m.get("ip"));
		assertEquals(24, m.get("mask"));
		assertEquals("zone", m.get("zone_name"));
	}

	@Test
	public void testStatefulPacketInspectionStatusLogV3() {
		String line = "3`0`2`1`000000`3070`20090911`18:45:52`10540`1024`2048``11364f4a-aa0e-42bc-92ea-0fe6a8e01744`0`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		assertEquals(3070, m.get("module_flag"));
		assertEquals("10540", m.get("duration"));
		assertEquals(1024L, m.get("drop_tcp_pps"));
		assertEquals(2048L, m.get("allow_tcp_pps"));
		assertEquals(null, m.get("zone_name"));
		assertEquals("11364f4a-aa0e-42bc-92ea-0fe6a8e01744", m.get("attack_id"));
		assertEquals("off", m.get("filter_status"));
	}

	@Test
	public void testHttpAccessAuthenticationIPLogV3() {
		String line = "3`0`2`1`000000`3062`20090911`18:45:52`10540`6`172.16.32.34`192.168.16.15`80`11`20090911`18:55:52`zone0`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		assertEquals(3062, m.get("module_flag"));
		assertEquals("10540", m.get("duration"));
		assertEquals("6", m.get("protocol"));
		assertEquals("172.16.32.34", m.get("src_ip"));
		assertEquals("192.168.16.15", m.get("dst_ip"));
		assertEquals(80, m.get("dst_port"));
		assertEquals(11, m.get("request_count"));
		assertEquals("20090911 18:55:52", m.get("expire_time"));
		assertEquals("zone0", m.get("zone_name"));
	}

	@Test
	public void testAntiSpoofingProtectionAuthLogV3() {
		String line = "3`0`2`1`000000`3052`20090911`18:45:52`10540`6`172.16.32.34`192.168.16.15`80`11`20090911`18:55:52`zone0`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		assertEquals(3052, m.get("module_flag"));
		assertEquals("10540", m.get("duration"));
		assertEquals("6", m.get("protocol"));
		assertEquals("172.16.32.34", m.get("src_ip"));
		assertEquals("192.168.16.15", m.get("dst_ip"));
		assertEquals(80, m.get("dst_port"));
		assertEquals(11, m.get("connection_count"));
		assertEquals("20090911 18:55:52", m.get("expire_time"));
		assertEquals("zone0", m.get("zone_name"));
	}

	@Test
	public void testAntiSpoofingProtectionStatusLogV3() {
		String line = "3`0`2`1`000000`3050`20090911`18:45:52`10540`1024`2048``11364f4a-aa0e-42bc-92ea-0fe6a8e01744`0`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		assertEquals(3050, m.get("module_flag"));
		assertEquals("1024", m.get("block_session"));
		assertEquals("2048", m.get("allow_session"));

		assertEquals(null, m.get("zone_name"));
		assertEquals("11364f4a-aa0e-42bc-92ea-0fe6a8e01744", m.get("attack_id"));
		assertEquals("off", m.get("filter_status"));
	}

	@Test
	public void testNetworkProtectionbySegmentStatusLogV3() {
		String line = "3`0`2`1`000000`3040`20090911`18:45:52`10540`100`200`100`200`100`200`100`200`10`20`10`20`10`20`10`20``11364f4a-aa0e-42bc-92ea-0fe6a8e01744`0`";

		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		assertEquals(3040, m.get("module_flag"));
		assertEquals("10540", m.get("duration"));

		assertEquals(100L, m.get("drop_tcp_pps"));
		assertEquals(200L, m.get("drop_tcp_bps"));
		assertEquals(100L, m.get("drop_udp_pps"));
		assertEquals(200L, m.get("drop_udp_bps"));
		assertEquals(100L, m.get("drop_icmp_pps"));
		assertEquals(200L, m.get("drop_icmp_bps"));
		assertEquals(100L, m.get("drop_etc_pps"));
		assertEquals(200L, m.get("drop_etc_bps"));
		assertEquals(10L, m.get("allow_tcp_pps"));
		assertEquals(20L, m.get("allow_tcp_bps"));
		assertEquals(10L, m.get("allow_udp_pps"));
		assertEquals(20L, m.get("allow_udp_bps"));
		assertEquals(10L, m.get("allow_icmp_pps"));
		assertEquals(20L, m.get("allow_icmp_bps"));
		assertEquals(10L, m.get("allow_etc_pps"));
		assertEquals(20L, m.get("allow_etc_bps"));

		assertEquals(null, m.get("zone_name"));
		assertEquals("11364f4a-aa0e-42bc-92ea-0fe6a8e01744", m.get("attack_id"));
		assertEquals("off", m.get("filter_status"));
	}

	@Test
	public void testUntrustedTrafficBlockFilterBlockLogV3() {
		String line = "3`0`2`1`000000`3031`20090911`18:45:52`10540`6`172.16.32.34`192.168.16.15`80`1234`16000`zone0`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		assertEquals(3031, m.get("module_flag"));
		assertEquals("10540", m.get("duration"));
		assertEquals("6", m.get("protocol"));
		assertEquals("172.16.32.34", m.get("src_ip"));
		assertEquals("192.168.16.15", m.get("dst_ip"));
		assertEquals(80, m.get("dst_port"));
		assertEquals(1234L, m.get("drop_packets"));
		assertEquals(16000L, m.get("drop_bytes"));
		assertEquals("zone0", m.get("zone_name"));
	}

	@Test
	public void testUntrustedTrafficBlockFilterStatusLogV3() {
		String line = "3`0`2`1`000000`3030`20090911`18:45:52`10540`100`200`100`200`100`200`100`200``11364f4a-aa0e-42bc-92ea-0fe6a8e01744`0`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		assertEquals(3030, m.get("module_flag"));
		assertEquals("10540", m.get("duration"));
		assertEquals(100L, m.get("drop_tcp_pps"));
		assertEquals(200L, m.get("drop_tcp_bps"));
		assertEquals(100L, m.get("drop_udp_pps"));
		assertEquals(200L, m.get("drop_udp_bps"));
		assertEquals(100L, m.get("drop_icmp_pps"));
		assertEquals(200L, m.get("drop_icmp_bps"));
		assertEquals(100L, m.get("drop_etc_pps"));
		assertEquals(200L, m.get("drop_etc_bps"));
		assertEquals(null, m.get("zone_name"));
		assertEquals("11364f4a-aa0e-42bc-92ea-0fe6a8e01744", m.get("attack_id"));
		assertEquals("off", m.get("filter_status"));
	}

	@Test
	public void testDpxIPSLogV3() {
		String line = "3`0`2`1`000000`3020`20110401`16:36:58`2`6`30.9.32.245`31142`74.117.56.131`4598`58`3003``DPX`2009`3`0800`00:10:F3:13:61:FC`2303097471`-1`s`0`hskim(2303097471)`0`0`0`0```";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		assertEquals(3020, m.get("module_flag"));
		assertEquals(2, m.get("severity"));
		assertEquals("6", m.get("protocol"));
		assertEquals("30.9.32.245", m.get("src_ip"));
		assertEquals(31142, m.get("src_port"));
		assertEquals("74.117.56.131", m.get("dst_ip"));
		assertEquals(4598, m.get("dst_port"));
		assertEquals(58, m.get("pkt_len"));
		assertEquals("허용", m.get("action"));
		assertEquals(null, m.get("user"));
		assertEquals("DPX", m.get("module_name"));
		assertEquals("2009", m.get("reason"));
		assertEquals("3", m.get("nif"));
		assertEquals("0800", m.get("eth_protocol"));
		assertEquals("00:10:F3:13:61:FC", m.get("src_mac"));
		assertEquals(2303097471L, m.get("rule_id"));

		assertEquals("-1", m.get("vlan_id"));
		assertEquals("최초 공격탐지", m.get("status"));
		assertEquals("0", m.get("duration"));
		assertEquals("hskim(2303097471)", m.get("msg"));
		assertEquals(0L, m.get("slice_seconds"));
		assertEquals(0L, m.get("threshold_packets"));
		assertEquals(0L, m.get("threshold_bytes"));
		assertEquals("0", m.get("attack_rate"));
		assertEquals(null, m.get("zone_name"));
		assertEquals(null, m.get("attack_id"));

	}

	@Test
	public void testManagementNetworkPortBridgeLogV3() {
		String line = "3`0`2`1`000000`3012`20100704`12:46:35`zone2`1`all`181`158`159`185`757`153`158`75`159`528`212`313`119`151`64`31`8`71`19`118`21`212`31`24`12`26`117`218`59`710`410`92`128`227`622`52`422`231`22`12`151`522`36`146`56`516`67`58`169`10`111`22`312`214`135`316`731`318`319`190`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals(3012, m.get("module_flag"));
		assertEquals("zone2", m.get("zone_name"));
		assertEquals("bridge", m.get("nif_type"));
		assertEquals("all", m.get("nif_name"));
		assertEquals(181, m.get("in_rx_tcp_bps"));
		assertEquals(158, m.get("in_rx_udp_bps"));
		assertEquals(159, m.get("in_rx_icmp_bps"));
		assertEquals(185, m.get("in_rx_etc_bps"));
		assertEquals(757, m.get("in_rx_total_bps"));
		assertEquals(153, m.get("in_rx_tcp_pps"));
		assertEquals(158, m.get("in_rx_udp_pps"));
		assertEquals(75, m.get("in_rx_icmp_pps"));
		assertEquals(159, m.get("in_rx_etc_pps"));
		assertEquals(528, m.get("in_rx_total_pps"));
		assertEquals(212, m.get("in_tx_tcp_bps"));
		assertEquals(313, m.get("in_tx_udp_bps"));
		assertEquals(119, m.get("in_tx_icmp_bps"));
		assertEquals(151, m.get("in_tx_etc_bps"));
		assertEquals(64, m.get("in_tx_total_bps"));
		assertEquals(31, m.get("in_tx_tcp_pps"));
		assertEquals(8, m.get("in_tx_udp_pps"));
		assertEquals(71, m.get("in_tx_icmp_pps"));
		assertEquals(19, m.get("in_tx_etc_pps"));
		assertEquals(118, m.get("in_tx_total_pps"));
		assertEquals(21, m.get("in_drop_tcp_bps"));
		assertEquals(212, m.get("in_drop_udp_bps"));
		assertEquals(31, m.get("in_drop_icmp_bps"));
		assertEquals(24, m.get("in_drop_etc_bps"));
		assertEquals(12, m.get("in_drop_total_bps"));
		assertEquals(26, m.get("in_drop_tcp_pps"));
		assertEquals(117, m.get("in_drop_udp_pps"));
		assertEquals(218, m.get("in_drop_icmp_pps"));
		assertEquals(59, m.get("in_drop_etc_pps"));
		assertEquals(710, m.get("in_drop_total_pps"));
		assertEquals(410, m.get("out_rx_tcp_bps"));
		assertEquals(92, m.get("out_rx_udp_bps"));
		assertEquals(128, m.get("out_rx_icmp_bps"));
		assertEquals(227, m.get("out_rx_etc_bps"));
		assertEquals(622, m.get("out_rx_total_bps"));
		assertEquals(52, m.get("out_rx_tcp_pps"));
		assertEquals(422, m.get("out_rx_udp_pps"));
		assertEquals(231, m.get("out_rx_icmp_pps"));
		assertEquals(22, m.get("out_rx_etc_pps"));
		assertEquals(12, m.get("out_rx_total_pps"));
		assertEquals(151, m.get("out_tx_tcp_bps"));
		assertEquals(522, m.get("out_tx_udp_bps"));
		assertEquals(36, m.get("out_tx_icmp_bps"));
		assertEquals(146, m.get("out_tx_etc_bps"));
		assertEquals(56, m.get("out_tx_total_bps"));
		assertEquals(516, m.get("out_tx_tcp_pps"));
		assertEquals(67, m.get("out_tx_udp_pps"));
		assertEquals(58, m.get("out_tx_icmp_pps"));
		assertEquals(169, m.get("out_tx_etc_pps"));
		assertEquals(10, m.get("out_tx_total_pps"));
		assertEquals(111, m.get("out_drop_tcp_bps"));
		assertEquals(22, m.get("out_drop_udp_bps"));
		assertEquals(312, m.get("out_drop_icmp_bps"));
		assertEquals(214, m.get("out_drop_etc_bps"));
		assertEquals(135, m.get("out_drop_total_bps"));
		assertEquals(316, m.get("out_drop_tcp_pps"));
		assertEquals(731, m.get("out_drop_udp_pps"));
		assertEquals(318, m.get("out_drop_icmp_pps"));
		assertEquals(319, m.get("out_drop_etc_pps"));
		assertEquals(190, m.get("out_drop_total_pps"));
	}

	@Test
	public void testManagementNetworkPortPhysicalLogV3() {
		String line = "3`0`2`1`000000`3012`20100704`12:46:35`zone1`0`eth0`28`38`19`18`77`13`18`7`19`28`5`6`7`8`9`4`5`6`7`7`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`0`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals(3012, m.get("module_flag"));
		assertEquals("zone1", m.get("zone_name"));
		assertEquals("physical", m.get("nif_type"));
		assertEquals("eth0", m.get("nif_name"));
		assertEquals(28, m.get("in_rx_tcp_bps"));
		assertEquals(38, m.get("in_rx_udp_bps"));
		assertEquals(19, m.get("in_rx_icmp_bps"));
		assertEquals(18, m.get("in_rx_etc_bps"));
		assertEquals(77, m.get("in_rx_total_bps"));
		assertEquals(13, m.get("in_rx_tcp_pps"));
		assertEquals(18, m.get("in_rx_udp_pps"));
		assertEquals(7, m.get("in_rx_icmp_pps"));
		assertEquals(19, m.get("in_rx_etc_pps"));
		assertEquals(28, m.get("in_rx_total_pps"));
		assertEquals(5, m.get("in_tx_tcp_bps"));
		assertEquals(6, m.get("in_tx_udp_bps"));
		assertEquals(7, m.get("in_tx_icmp_bps"));
		assertEquals(8, m.get("in_tx_etc_bps"));
		assertEquals(9, m.get("in_tx_total_bps"));
		assertEquals(4, m.get("in_tx_tcp_pps"));
		assertEquals(5, m.get("in_tx_udp_pps"));
		assertEquals(6, m.get("in_tx_icmp_pps"));
		assertEquals(7, m.get("in_tx_etc_pps"));
		assertEquals(7, m.get("in_tx_total_pps"));
		assertEquals(0, m.get("in_drop_tcp_bps"));
		assertEquals(0, m.get("in_drop_udp_bps"));
		assertEquals(0, m.get("in_drop_icmp_bps"));
		assertEquals(0, m.get("in_drop_etc_bps"));
		assertEquals(0, m.get("in_drop_total_bps"));
		assertEquals(0, m.get("in_drop_tcp_pps"));
		assertEquals(0, m.get("in_drop_udp_pps"));
		assertEquals(0, m.get("in_drop_icmp_pps"));
		assertEquals(0, m.get("in_drop_etc_pps"));
		assertEquals(0, m.get("in_drop_total_pps"));
		assertEquals(0, m.get("out_rx_tcp_bps"));
		assertEquals(0, m.get("out_rx_udp_bps"));
		assertEquals(0, m.get("out_rx_icmp_bps"));
		assertEquals(0, m.get("out_rx_etc_bps"));
		assertEquals(0, m.get("out_rx_total_bps"));
		assertEquals(0, m.get("out_rx_tcp_pps"));
		assertEquals(0, m.get("out_rx_udp_pps"));
		assertEquals(0, m.get("out_rx_icmp_pps"));
		assertEquals(0, m.get("out_rx_etc_pps"));
		assertEquals(0, m.get("out_rx_total_pps"));
		assertEquals(0, m.get("out_tx_tcp_bps"));
		assertEquals(0, m.get("out_tx_udp_bps"));
		assertEquals(0, m.get("out_tx_icmp_bps"));
		assertEquals(0, m.get("out_tx_etc_bps"));
		assertEquals(0, m.get("out_tx_total_bps"));
		assertEquals(0, m.get("out_tx_tcp_pps"));
		assertEquals(0, m.get("out_tx_udp_pps"));
		assertEquals(0, m.get("out_tx_icmp_pps"));
		assertEquals(0, m.get("out_tx_etc_pps"));
		assertEquals(0, m.get("out_tx_total_pps"));
		assertEquals(0, m.get("out_drop_tcp_bps"));
		assertEquals(0, m.get("out_drop_udp_bps"));
		assertEquals(0, m.get("out_drop_icmp_bps"));
		assertEquals(0, m.get("out_drop_etc_bps"));
		assertEquals(0, m.get("out_drop_total_bps"));
		assertEquals(0, m.get("out_drop_tcp_pps"));
		assertEquals(0, m.get("out_drop_udp_pps"));
		assertEquals(0, m.get("out_drop_icmp_pps"));
		assertEquals(0, m.get("out_drop_etc_pps"));
		assertEquals(0, m.get("out_drop_total_pps"));
	}

	@Test
	public void testManagementStatLogV3() {
		String line = "3`0`2`1`000000`3011`20100704`12:46:35`Status로그`2`22`30`8`378000`224000`53`19`OFF`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals(3011, m.get("module_flag"));
		assertEquals("Status로그", m.get("module_name"));
		assertEquals(2, m.get("cpu"));
		assertEquals(22, m.get("mem"));
		assertEquals(30, m.get("hdd"));
		assertEquals(8, m.get("session"));
		assertEquals(378000L, m.get("in_data"));
		assertEquals(224000L, m.get("out_data"));
		assertEquals(53L, m.get("in_pkt"));
		assertEquals(19L, m.get("out_pkt"));
		assertEquals("OFF", m.get("ha"));
	}

	@Test
	public void testManagementOperaionLogV3() {
		String line = "3`0`2`1`000000`3010`20100704`12:46:35`4``````3001``운영로그`관리자가 로그인했습니다 : apcadmin`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals(3010, m.get("module_flag"));
		assertEquals(4, m.get("severity"));
		assertEquals(null, m.get("protocol"));
		assertEquals(null, m.get("src_ip"));
		assertEquals(null, m.get("src_port"));
		assertEquals(null, m.get("dst_ip"));
		assertEquals(null, m.get("dst_port"));
		assertEquals("3001", m.get("action"));
		assertEquals(null, m.get("user"));
		assertEquals("운영로그", m.get("module_name"));
		assertEquals("관리자가 로그인했습니다 : apcadmin", m.get("description"));
	}

	@Test
	public void testSystemQuarantineLogV3() {
		String line = "3`0`2`1`000000`1160`20071001`16:55:10`0``1.1.1.1`123`2.2.2.2`321`0``IPS`시스템을 격리 해제했습니다.`기관코드`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals(1160, m.get("module_flag"));
		assertEquals(0, m.get("severity"));
		assertEquals(null, m.get("protocol"));
		assertEquals("1.1.1.1", m.get("src_ip"));
		assertEquals(123, m.get("src_port"));
		assertEquals("2.2.2.2", m.get("dst_ip"));
		assertEquals(321, m.get("dst_port"));
		assertEquals("격리", m.get("action"));
		assertEquals(null, m.get("user"));
		assertEquals("IPS", m.get("module_name"));
		assertEquals("시스템을 격리 해제했습니다.", m.get("description"));
		assertEquals("기관코드", m.get("code"));
	}

	@Test
	public void testProxyLogV3() {
		String line = "3`0`2`1`000000`1150`20080328`01:57:51`4``192.168.1.1````3003``프록시 인증`sshong 인증되었습니다.`기관코드`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals(1150, m.get("module_flag"));
		assertEquals(4, m.get("severity"));
		assertEquals(null, m.get("protocol"));
		assertEquals("192.168.1.1", m.get("src_ip"));
		assertEquals(null, m.get("src_port"));
		assertEquals(null, m.get("dst_ip"));
		assertEquals(null, m.get("dst_port"));
		assertEquals("ACT_PASS", m.get("action"));
		assertEquals(null, m.get("user"));
		assertEquals("프록시 인증", m.get("module_name"));
		assertEquals("sshong 인증되었습니다.", m.get("description"));
		assertEquals("기관코드", m.get("code"));
	}

	@Test
	public void testLbqosLogV3() {
		String line = "3`0`2`1`000000`1141`20080328`01:57:51`4`17`192.168.1.1`4993`211.41.4.33`13568`3007``대용량 웹 트래픽`Apply 제한QoS`1234567`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals(1141, m.get("module_flag"));
		assertEquals(4, m.get("severity"));
		assertEquals("17", m.get("protocol"));
		assertEquals("192.168.1.1", m.get("src_ip"));
		assertEquals(4993, m.get("src_port"));
		assertEquals("211.41.4.33", m.get("dst_ip"));
		assertEquals(13568, m.get("dst_port"));
		assertEquals("3007", m.get("action"));
		assertEquals("대용량 웹 트래픽", m.get("module_name"));
		assertEquals("Apply 제한QoS", m.get("description"));
		assertEquals("1234567", m.get("code"));
	}

	@Test
	public void testQosLogV3() {
		String line = "3`0`2`1`000000`1140`20100526`12:46:35`0``````3009``QoS 모니터`100K`eth2`64000`1000` 기관코드`";

		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals(1140, m.get("module_flag"));
		assertEquals(0, m.get("severity"));
		assertEquals(null, m.get("protocol"));
		assertEquals(null, m.get("src_ip"));
		assertEquals(null, m.get("src_port"));
		assertEquals(null, m.get("dst_ip"));
		assertEquals(null, m.get("dst_port"));
		assertEquals("3009", m.get("action"));
		assertEquals(null, m.get("user"));
		assertEquals("QoS 모니터", m.get("module_name"));

		assertEquals("100K", m.get("qos_name"));
		assertEquals("eth2", m.get("eth_name"));
		assertEquals(64000, m.get("bps"));
		assertEquals(1000, m.get("pps"));
		assertEquals(" 기관코드", m.get("code"));
	}

	@Test
	public void testInternetAccessControlLogV3() {
		String line = "3`0`2`1`000000`1120`20080328`01:57:51`4`17`192.168.1.1`4993`211.41.4.33`13568`4``IAC`00:10:f3:09:2c:34`1234567`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals(1120, m.get("module_flag"));
		assertEquals(4, m.get("severity"));
		assertEquals("17", m.get("protocol"));
		assertEquals("192.168.1.1", m.get("src_ip"));
		assertEquals(4993, m.get("src_port"));
		assertEquals("211.41.4.33", m.get("dst_ip"));
		assertEquals(13568, m.get("dst_port"));
		assertEquals("차단(미설치)", m.get("action"));
		assertEquals(null, m.get("user"));
		assertEquals("IAC", m.get("module_name"));
		assertEquals("00:10:f3:09:2c:34", m.get("mac"));
		assertEquals("1234567", m.get("code"));
	}

	@Test
	public void testInternetAccessControlAppLogV3() {
		String line = "3`0`2`1`000000`1121`20080328`01:57:51`4``192.168.1.1````4``IAC`[BotNet]Win32.Madang.A`기관코드`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals(1121, m.get("module_flag"));
		assertEquals("192.168.1.1", m.get("src_ip"));
		assertEquals("차단(미설치)", m.get("action"));
		assertEquals("IAC", m.get("module_name"));
		assertEquals("[BotNet]Win32.Madang.A", m.get("group_name"));
		assertEquals("기관코드", m.get("code"));
	}

	@Test
	public void testDnsLogV3() {
		String line = "3`0`2`1`000000`1110`20080109`18:04:18`4`17`10.0.1.1`1048`210.181.4.25`53`3001``DNS 필터`Private IP Query`(ahnlab.co.kr->172.31.11.0)`기관코드`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals(1110, m.get("module_flag"));
		assertEquals(4, m.get("severity"));
		assertEquals("17", m.get("protocol"));
		assertEquals("10.0.1.1", m.get("src_ip"));
		assertEquals(1048, m.get("src_port"));
		assertEquals("210.181.4.25", m.get("dst_ip"));
		assertEquals(53, m.get("dst_port"));
		assertEquals("3001", m.get("action"));
		assertEquals("DNS 필터", m.get("module_name"));
		assertEquals("Private IP Query", m.get("reason"));
		assertEquals("(ahnlab.co.kr->172.31.11.0)", m.get("description"));
		assertEquals("기관코드", m.get("code"));
	}

	@Test
	public void testIpsLogV3() {
		String line = "3`0`2`1`42c0cd`1100`20131028`19:24:50`4`6`192.168.7.101`60465`31.13.68.16`443`3003``IPS`2009`1`0800`E8:40:F2:17:E0:67`780000502`-1`social_url_facebook(HTTPS)``";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("42c0cd", m.get("utm_id"));

		// check log data
		assertEquals(1100, m.get("module_flag"));
		assertEquals(4, m.get("severity"));
		assertEquals("6", m.get("protocol"));
		assertEquals("192.168.7.101", m.get("src_ip"));
		assertEquals(60465, m.get("src_port"));
		assertEquals("31.13.68.16", m.get("dst_ip"));
		assertEquals(443, m.get("dst_port"));

		assertEquals("허용", m.get("action"));
		assertEquals(null, m.get("user"));
		assertEquals("IPS", m.get("module_name"));
		assertEquals("2009", m.get("reason"));
		assertEquals("1", m.get("nif"));
		assertEquals("0800", m.get("eth_protocol"));
		assertEquals("E8:40:F2:17:E0:67", m.get("src_mac"));
		assertEquals("780000502", m.get("rule_id"));
		assertEquals("-1", m.get("vlan_id"));
		assertEquals("social_url_facebook(HTTPS)", m.get("msg"));
		assertEquals(null, m.get("code"));
	}

	@Test
	public void testAppFilterLogV3() {
		String line = "3`0`2`1`000000`1070`20071023`17:46:34`0``````3009``콘텐츠 필터`FTP`출발지(172.16.104.2:46235)에서 목적지(202.79.178.98:21)로 연결이 종료되었습니다.`기관코드`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals(1070, m.get("module_flag"));
		assertEquals(0, m.get("severity"));
		assertEquals(null, m.get("protocol"));
		assertEquals(null, m.get("src_ip"));
		assertEquals(null, m.get("src_port"));
		assertEquals(null, m.get("dst_ip"));
		assertEquals(null, m.get("dst_port"));
		assertEquals("3009", m.get("action"));
		assertEquals("콘텐츠 필터", m.get("module_name"));
		assertEquals("FTP", m.get("ap_protocol"));
		assertEquals("출발지(172.16.104.2:46235)에서 목적지(202.79.178.98:21)로 연결이 종료되었습니다.", m.get("description"));
		assertEquals("기관코드", m.get("code"));
	}

	@Test
	public void testWebFilterLogV3() {
		String line = "3`0`2`1`000000`1050`20071029`12:48:28`4`6`172.16.108.146`3561`61.97.65.4`80`3001``웹사이트 필터`UserURL`UserURL`http://www.empas.com/empaspcid.js``";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals(1050, m.get("module_flag"));
		assertEquals(4, m.get("severity"));
		assertEquals("6", m.get("protocol"));
		assertEquals("172.16.108.146", m.get("src_ip"));
		assertEquals(3561, m.get("src_port"));
		assertEquals("61.97.65.4", m.get("dst_ip"));
		assertEquals(80, m.get("dst_port"));
		assertEquals("3001", m.get("action"));
		assertEquals("웹사이트 필터", m.get("module_name"));
		assertEquals("UserURL", m.get("wf_type"));
		assertEquals("UserURL", m.get("reason"));
		assertEquals("http://www.empas.com/empaspcid.js", m.get("url"));
		assertEquals(null, m.get("code"));
	}

	@Test
	public void testStatLogV3() {
		String line = "3`0`2`1`42c0cd`1011`20131029`16:00:47`Status로그`13`20`4`1552`50381760`50501736`6933`7036`OFF``";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("42c0cd", m.get("utm_id"));

		// check log data
		assertEquals(1011, m.get("module_flag"));
		assertEquals("Status로그", m.get("module_name"));
		assertEquals(13, m.get("cpu"));
		assertEquals(20, m.get("mem"));
		assertEquals(4, m.get("hdd"));
		assertEquals("1552", m.get("session"));
		assertEquals(50381760L, m.get("in_data"));
		assertEquals(50501736L, m.get("out_data"));
		assertEquals(6933L, m.get("in_pkt"));
		assertEquals(7036L, m.get("out_pkt"));
		assertEquals("OFF", m.get("ha"));
		assertEquals(null, m.get("code"));
	}

	@Test
	public void testOperationLogV3() {
		String line = "3`0`2`1`42c0cd`1010`20131029`14:52:04`0``````2``운영 로그`관리자가 로그아웃했습니다.(아이디: admin, IP 주소: 172.16.108.152)``";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("42c0cd", m.get("utm_id"));

		// check log data
		assertEquals(1010, m.get("module_flag"));
		assertEquals(0, m.get("severity"));
		assertEquals(null, m.get("protocol"));
		assertEquals(null, m.get("src_ip"));
		assertEquals(null, m.get("src_port"));
		assertEquals(null, m.get("dst_ip"));
		assertEquals(null, m.get("dst_port"));
		assertEquals("2", m.get("action"));
		assertEquals(null, m.get("user"));
		assertEquals("운영 로그", m.get("module_name"));
		assertEquals("관리자가 로그아웃했습니다.(아이디: admin, IP 주소: 172.16.108.152)", m.get("description"));
		assertEquals(null, m.get("code"));
	}

	@Test
	public void testDenyLogV3() {
		String line = "3`0`1`1`42c0cd`1021`20131029`13:35:12`2`17`UTM_DEFAULT`218.151.229.111`54571`211.170.44.202`161`eth2(Out)`eth3(DMZ)````106`1````````";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(1, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("42c0cd", m.get("utm_id"));

		// check log data
		assertEquals(1021, m.get("module_flag"));
		assertEquals("Deny", m.get("logtype"));
		assertEquals("17", m.get("protocol"));
		assertEquals("UTM_DEFAULT", m.get("policy_id"));
		assertEquals("218.151.229.111", m.get("src_ip"));
		assertEquals(54571, m.get("src_port"));
		assertEquals("211.170.44.202", m.get("dst_ip"));
		assertEquals(161, m.get("dst_port"));
		assertEquals("eth2(Out)", m.get("in_nic"));
		assertEquals("eth3(DMZ)", m.get("out_nic"));
		assertEquals(null, m.get("nat_type"));
		assertEquals(null, m.get("nat_ip"));
		assertEquals(null, m.get("nat_port"));
		assertEquals(106L, m.get("sent_data"));
		assertEquals(1L, m.get("sent_pkt"));
		assertEquals(null, m.get("rcvd_data"));
		assertEquals(null, m.get("rcvd_pkt"));
		assertEquals(null, m.get("duration"));
		assertEquals(null, m.get("state"));
		assertEquals(null, m.get("reason"));
		assertEquals(null, m.get("code"));
		assertEquals(null, m.get("tcp_flag"));
	}

	@Test
	public void testAllowAndExpireNatLogV3() {
		String line = "3`0`1`1`42c0cd`1020`20131029`13:35:15`3`6`130406105149`192.168.5.108`52858`59.106.153.9`80`eth0(Inside5)`eth2(Out)`SNAT`211.40.7.130`29955`590`6`1277`5``31`1``S sa A / fa A FA+ a`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(1, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("42c0cd", m.get("utm_id"));

		// check log data
		assertEquals(1020, m.get("module_flag"));
		assertEquals("Expire", m.get("logtype"));
		assertEquals("6", m.get("protocol"));
		assertEquals("130406105149", m.get("policy_id"));
		assertEquals("192.168.5.108", m.get("src_ip"));
		assertEquals(52858, m.get("src_port"));
		assertEquals("59.106.153.9", m.get("dst_ip"));
		assertEquals(80, m.get("dst_port"));
		assertEquals("eth0(Inside5)", m.get("in_nic"));
		assertEquals("eth2(Out)", m.get("out_nic"));
		assertEquals("SNAT", m.get("nat_type"));
		assertEquals("211.40.7.130", m.get("nat_ip"));
		assertEquals(29955, m.get("nat_port"));
		assertEquals(590L, m.get("sent_data"));
		assertEquals(6L, m.get("sent_pkt"));
		assertEquals(1277L, m.get("rcvd_data"));
		assertEquals(5L, m.get("rcvd_pkt"));
		assertEquals(null, m.get("duration"));
		assertEquals("31", m.get("state"));
		assertEquals("1", m.get("reason"));
		assertEquals(null, m.get("code"));
		assertEquals("S sa A / fa A FA+ a", m.get("tcp_flag"));

	}

	@Test
	public void testAllowAndExpireLogV3() {
		String line = "3`0`1`1`42c0cd`1020`20131029`13:35:15`3`6`130406172403`218.151.228.151`64670`211.170.44.1`80`eth2(Out)`eth3(DMZ)````1014`5`292`3``15`2``S sa A / RA";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(3, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(1, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("42c0cd", m.get("utm_id"));

		// check log data
		assertEquals(1020, m.get("module_flag"));
		assertEquals("Expire", m.get("logtype"));
		assertEquals("6", m.get("protocol"));
		assertEquals("130406172403", m.get("policy_id"));
		assertEquals("218.151.228.151", m.get("src_ip"));
		assertEquals(64670, m.get("src_port"));
		assertEquals("211.170.44.1", m.get("dst_ip"));
		assertEquals(80, m.get("dst_port"));
		assertEquals("eth2(Out)", m.get("in_nic"));
		assertEquals("eth3(DMZ)", m.get("out_nic"));
		assertEquals(null, m.get("nat_type"));
		assertEquals(null, m.get("nat_ip"));
		assertEquals(null, m.get("nat_port"));
		assertEquals(1014L, m.get("sent_data"));
		assertEquals(5L, m.get("sent_pkt"));
		assertEquals(292L, m.get("rcvd_data"));
		assertEquals(3L, m.get("rcvd_pkt"));
		assertEquals(null, m.get("duration"));
		assertEquals("15", m.get("state"));
		assertEquals("2", m.get("reason"));
		assertEquals(null, m.get("code"));
		assertEquals("S sa A / RA", m.get("tcp_flag"));

	}

	@Test
	public void testDnsFilter() {
		String line = "1`0`2`1`000000`11`20080109`18:04:18`Low`17`10.0.1.1`1048`210.181.4.25`53`3001``DNS 필터`Private IP Query`(ahnlab.co.kr->172.31.11.0)`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

	}

	@Test
	public void testOperationLog() {
		String line = "1`0`2`1`000000`1`20071026`12:48:08`0````3009``운영 로그`TrusGuard UTM의 정책을 적용했습니다.`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals("3009", m.get("action"));
		assertEquals("운영 로그", m.get("module_name"));
		assertEquals("TrusGuard UTM의 정책을 적용했습니다.", m.get("description"));
	}

	@Test
	public void testStatLog() {
		String line = "1`0`2`1`0bf075`1`20071025`18:00:44`0````3009``Operation Log`CPU: 19.280720, Memory: 22.252111, HDD: 30, Connections: 28, IN: 130.0Kbps, OUT: 68.3Kbps, IN:128 pps, OUT:41 pps, HA: OFF`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("0bf075", m.get("utm_id"));

		// check log data
		assertEquals("3009", m.get("action"));
		assertEquals("Operation Log", m.get("module_name"));
		assertEquals(
				"CPU: 19.280720, Memory: 22.252111, HDD: 30, Connections: 28, IN: 130.0Kbps, OUT: 68.3Kbps, IN:128 pps, OUT:41 pps, HA: OFF",
				m.get("description"));
	}

	@Test
	public void testAllowAndExpireLog() {
		String line = "1`0`1`1`000000`20071025`17:46:26`Expire`6`UTM_ADMINHOST`172.16.108.152`4430`172.16.108.211`50005`eth0`unknown````1021`8`724`7`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(1, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check firewall log data // check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(1, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		assertEquals("Expire", m.get("logtype"));
		assertEquals("6", m.get("protocol"));
		assertEquals("UTM_ADMINHOST", m.get("policy_id"));
		assertEquals("172.16.108.152", m.get("src_ip"));
		assertEquals(4430, m.get("src_port"));
		assertEquals("172.16.108.211", m.get("dst_ip"));
		assertEquals(50005, m.get("dst_port"));
		assertEquals("eth0", m.get("in_nic"));
		assertEquals("unknown", m.get("out_nic"));
		assertNull(m.get("nat_ip"));
		assertNull(m.get("nat_port"));
		assertEquals(1021L, m.get("sent_data"));
		assertEquals(8L, m.get("sent_pkt"));
		assertEquals(724L, m.get("rcvd_data"));
		assertEquals(7L, m.get("rcvd_pkt"));
	}

	@Test
	public void testAllowAndExpireNatLog() {
		String line = "1`0`1`1`000000`20071025`17:46:26`Expire`6`UTM_ADMINHOST`172.16.108.152`4430`172.16.108.211`50005`eth0`unknown`SNAT`210.16.108.194`11005`1021`8`724`7`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(1, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals("Expire", m.get("logtype"));
		assertEquals("6", m.get("protocol"));
		assertEquals("UTM_ADMINHOST", m.get("policy_id"));
		assertEquals("172.16.108.152", m.get("src_ip"));
		assertEquals(4430, m.get("src_port"));
		assertEquals("172.16.108.211", m.get("dst_ip"));
		assertEquals(50005, m.get("dst_port"));
		assertEquals("eth0", m.get("in_nic"));
		assertEquals("unknown", m.get("out_nic"));
		assertEquals("SNAT", m.get("nat_type"));
		assertEquals("210.16.108.194", m.get("nat_ip"));
		assertEquals(11005, m.get("nat_port"));
		assertEquals(1021L, m.get("sent_data"));
		assertEquals(8L, m.get("sent_pkt"));
		assertEquals(724L, m.get("rcvd_data"));
		assertEquals(7L, m.get("rcvd_pkt"));

	}

	@Test
	public void testDenyLog() {
		String line = "1`0`1`1`000000`20071025`17:56:38`Deny`17`UTM_DEFAULT`172.16.104.4`137`172.16.255.255`137`eth0`unknown````19968`1```";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(1, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals("Deny", m.get("logtype"));
		assertEquals("17", m.get("protocol"));
		assertEquals("UTM_DEFAULT", m.get("policy_id"));
		assertEquals("172.16.104.4", m.get("src_ip"));
		assertEquals(137, m.get("src_port"));
		assertEquals("172.16.255.255", m.get("dst_ip"));
		assertEquals(137, m.get("dst_port"));
		assertEquals("eth0", m.get("in_nic"));
		assertEquals("unknown", m.get("out_nic"));
		assertNull(m.get("nat_type"));
		assertNull(m.get("nat_ip"));
		assertNull(m.get("nat_port"));
		assertEquals(19968L, m.get("sent_data"));
		assertEquals(1L, m.get("sent_pkt"));
		assertNull(m.get("rcvd_data"));
		assertNull(m.get("rcvd_pkt"));

	}

	@Test
	public void testAppFilterLog() {
		String line = "1`0`2`1`000000`6`20071023`17:46:34`0````3009``콘텐츠 필터`FTP`출발지(172.16.104.2:46235)에서 목적지(202.79.178.98:21)로 연결이 종료되었습니다.`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals(6, m.get("module_flag"));
		assertEquals(0, m.get("severity"));
		assertEquals("3009", m.get("action"));
		assertEquals("콘텐츠 필터", m.get("module_name"));
		assertEquals("FTP", m.get("ap_protocol"));
		assertEquals("출발지(172.16.104.2:46235)에서 목적지(202.79.178.98:21)로 연결이 종료되었습니다.", m.get("description"));
	}

	@Test
	public void testWebFilterLog() {
		String line = "1`0`2`1`000000`4`20071026`13:05:27`Low`6`172.16.108.144`3427`61.97.65.4`80`3001``웹사이트 필터`UserURL`UserURL`[http://www.empas.com/]`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals("Low", m.get("severity"));
		assertEquals("6", m.get("protocol"));
		assertEquals("172.16.108.144", m.get("src_ip"));
		assertEquals(3427, m.get("src_port"));
		assertEquals("61.97.65.4", m.get("dst_ip"));
		assertEquals(80, m.get("dst_port"));
		assertEquals("3001", m.get("action"));
		assertEquals("웹사이트 필터", m.get("module_name"));
		assertEquals("UserURL", m.get("wf_type"));
		assertEquals("UserURL", m.get("reason"));
		assertEquals("http://www.empas.com/", m.get("url"));
	}

	@Test
	public void testSmtpPop3Log() {
		String line = "1`0`2`1`000000`2`20071031`12:33:40`HIGH`6`60.1.100.6`49566`172.16.108.152`25`3001``바이러스 차단`1`EICAR_Test_File`eicar_com.zip`circleo@gmail.com`circleo@kornet.net`FW: 광고 ..테스트 메일`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals("HIGH", m.get("severity"));
		assertEquals("6", m.get("protocol"));
		assertEquals("60.1.100.6", m.get("src_ip"));
		assertEquals(49566, m.get("src_port"));
		assertEquals("172.16.108.152", m.get("dst_ip"));
		assertEquals(25, m.get("dst_port"));
		assertEquals("3001", m.get("action"));
		assertEquals("바이러스 차단", m.get("module_name"));
		assertEquals("1", m.get("virus_filter"));
		assertEquals("EICAR_Test_File", m.get("virus_name"));
		assertEquals("eicar_com.zip", m.get("virus_fname"));
		assertEquals("circleo@gmail.com", m.get("sender_addr"));
		assertEquals("circleo@kornet.net", m.get("recipients_addr"));
		assertEquals("FW: 광고 ..테스트 메일", m.get("subject"));
	}

	@Test
	public void testFtpLog() {
		String line = "1`0`2`1`000000`2`20071030`14:31:48`HIGH`6`60.1.100.6`49566`172.16.108.152`21`3001``바이러스 차단`1`EICAR_Test_File`eicar_com.zip`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals("HIGH", m.get("severity"));
		assertEquals("6", m.get("protocol"));
		assertEquals("60.1.100.6", m.get("src_ip"));
		assertEquals(49566, m.get("src_port"));
		assertEquals("172.16.108.152", m.get("dst_ip"));
		assertEquals(21, m.get("dst_port"));
		assertEquals("3001", m.get("action"));
		assertEquals("바이러스 차단", m.get("module_name"));
		assertEquals("1", m.get("virus_filter"));
		assertEquals("EICAR_Test_File", m.get("virus_name"));
		assertEquals("eicar_com.zip", m.get("virus_fname"));
	}

	@Test
	public void testHttpLog() {
		String line = "1`0`2`1`000000`2`20071030`12:58:43`HIGH`6`172.16.108.152`2118`88.198.38.136`80`3001``바이러스 차단`AntiVirus(V3)`EICAR_Test_File`[http://www.eicar.org/download/eicarcom2.zip]`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals("HIGH", m.get("severity"));
		assertEquals("6", m.get("protocol"));
		assertEquals("172.16.108.152", m.get("src_ip"));
		assertEquals(2118, m.get("src_port"));
		assertEquals("88.198.38.136", m.get("dst_ip"));
		assertEquals(80, m.get("dst_port"));
		assertEquals("3001", m.get("action"));
		assertEquals("바이러스 차단", m.get("module_name"));
		assertEquals("AntiVirus(V3)", m.get("virus_filter"));
		assertEquals("EICAR_Test_File", m.get("virus_name"));
		assertEquals("http://www.eicar.org/download/eicarcom2.zip", m.get("virus_url"));
	}

	@Test
	public void testSpamLog() {
		String line = "1`0`2`1`000000`3`20071009`11:35:41`Low`6`172.16.104.1`3748`211.48.62.132`110`3003``스팸 메일 차단`2`0`circleo@gmail.com`circleo@kornet.net`FW: 광고 ..테스트 메일`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals("Low", m.get("severity"));
		assertEquals("6", m.get("protocol"));
		assertEquals("172.16.104.1", m.get("src_ip"));
		assertEquals(3748, m.get("src_port"));
		assertEquals("211.48.62.132", m.get("dst_ip"));
		assertEquals(110, m.get("dst_port"));
		assertEquals("3003", m.get("action"));
		assertEquals("스팸 메일 차단", m.get("module_name"));
		assertEquals("2", m.get("spam_filter"));
		assertEquals("0", m.get("send_spam_log"));
		assertEquals("circleo@gmail.com", m.get("sender_addr"));
		assertEquals("circleo@kornet.net", m.get("recipients_addr"));
		assertEquals("FW: 광고 ..테스트 메일", m.get("subject"));
	}

	@Test
	public void testSslVpnLog() {
		String line = "1`0`2`1`000000`8`20071030`15:40:18`0`6`192.168.0.6`3021`60.1.100.6`22`3009`user1`SSL VPN`Session closed`Disabled`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals("6", m.get("protocol"));
		assertEquals("192.168.0.6", m.get("src_ip"));
		assertEquals(3021, m.get("src_port"));
		assertEquals("60.1.100.6", m.get("dst_ip"));
		assertEquals(22, m.get("dst_port"));
		assertEquals("3009", m.get("action"));
		assertEquals("user1", m.get("user"));
		assertEquals("SSL VPN", m.get("module_name"));
		assertEquals("Session closed", m.get("event"));
		assertEquals("Disabled", m.get("epsec"));
	}

	@Test
	public void testDdosLog() {
		String line = "1`0`2`1`000000`9`20070515`15:45:29`2`17`5.5.5.1`14194`4.4.4.5`31335`3001``IPS`2012`3`0800`00:03:47:B5:B0:7`10232`65535` DDOS Trin00 Daemon to Master *HELLO* message detected`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals("17", m.get("protocol"));
		assertEquals("5.5.5.1", m.get("src_ip"));
		assertEquals(14194, m.get("src_port"));
		assertEquals("4.4.4.5", m.get("dst_ip"));
		assertEquals(31335, m.get("dst_port"));
		assertEquals("3001", m.get("action"));
		assertEquals("IPS", m.get("module_name"));
		assertEquals("2012", m.get("reason"));
		assertEquals("3", m.get("nif"));
		assertEquals("0800", m.get("eth_protocol"));
		assertEquals("00:03:47:B5:B0:7", m.get("src_mac"));
		assertEquals("10232", m.get("rule_id"));
		assertEquals("65535", m.get("vlan_id"));
		assertEquals(" DDOS Trin00 Daemon to Master *HELLO* message detected", m.get("msg"));
	}

	@Test
	public void testExploitLog() {
		String line = "1`0`2`1`000000`9`20070515`15:45:58`1`17`5.5.5.1`14508`4.4.4.5`635`3001``IPS`2012`3`0800`00:03:47:B5:B0:7`10315`65535` EXPLOIT x86 Linux mountd overflow";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals("17", m.get("protocol"));
		assertEquals("5.5.5.1", m.get("src_ip"));
		assertEquals(14508, m.get("src_port"));
		assertEquals("4.4.4.5", m.get("dst_ip"));
		assertEquals(635, m.get("dst_port"));
		assertEquals("3001", m.get("action"));
		assertEquals("IPS", m.get("module_name"));
		assertEquals("2012", m.get("reason"));
		assertEquals("3", m.get("nif"));
		assertEquals("0800", m.get("eth_protocol"));
		assertEquals("00:03:47:B5:B0:7", m.get("src_mac"));
		assertEquals("10315", m.get("rule_id"));
		assertEquals("65535", m.get("vlan_id"));
		assertEquals(" EXPLOIT x86 Linux mountd overflow", m.get("msg"));
	}

	@Ignore
	@Test
	public void testPortScanLog() {
		// this log has invalid delimiter formatting
		String line = "1`0`2`1`000000`9`20071025`09:16:38`3`6`172.16.108.144`3204`121.140.211.81`9101`3003``IPS`2012`1`0800`00:0F:B5:4D:84:EB` `1331003`-1`anomaly scan`";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals("6", m.get("protocol"));
		assertEquals("172.16.108.144", m.get("src_ip"));
		assertEquals(3204, m.get("src_port"));
		assertEquals("121.140.211.81", m.get("dst_ip"));
		assertEquals(9101, m.get("dst_port"));
		assertEquals("3003", m.get("action"));
		assertEquals("IPS", m.get("module_name"));
		assertEquals("2012", m.get("reason"));
		assertEquals("1", m.get("nif"));
		assertEquals("0800", m.get("eth_protocol"));
		assertEquals("00:0F:B5:4D:84:EB", m.get("src_mac"));
		assertEquals("13331003", m.get("rule_id"));
		assertEquals(" ", m.get("vlan_id"));
		assertEquals("anomaly scan", m.get("msg"));
	}

	@Test
	public void testInternetAccessControlLog() {
		String line = "1`0`2`1`000000`12`20080328`01:57:51`4`17`192.168.1.1`4993`211.41.4.33`13568`4``IAC`00:10:f3:09:2c:34";
		Map<String, Object> m = new TrusGuardLogParser().parse(line(line));

		// check log header
		assertNotNull(m);
		assertEquals(1, m.get("version"));
		assertEquals(0, m.get("encrypt"));
		assertEquals(2, m.get("type"));
		assertEquals(1, m.get("count"));
		assertEquals("000000", m.get("utm_id"));

		// check log data
		assertEquals(4, m.get("severity"));
		assertEquals("17", m.get("protocol"));
		assertEquals("192.168.1.1", m.get("src_ip"));
		assertEquals(4993, m.get("src_port"));
		assertEquals("211.41.4.33", m.get("dst_ip"));
		assertEquals(13568, m.get("dst_port"));
		assertEquals("4", m.get("action"));
		assertEquals("IAC", m.get("module_name"));
		assertEquals("00:10:f3:09:2c:34", m.get("mac"));
	}

	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
