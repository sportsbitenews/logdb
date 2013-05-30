/*
 * Copyright 2013 Eediom Inc.
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
package org.araqne.logparser.syslog.secui;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * @since 1.9.2
 * @author xeraph
 * 
 */
public class Mf2LogParserTest {
	@Test
	public void parseSample1() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:35.002198Z [ha_status] [23.1.3.10] "
				+ "2013-05-28 09:56:35,1,localhost-M,16,master"));
		assertEquals("ha_status", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:35", m.get("event_at"));
		assertEquals("1,localhost-M,16,master", m.get("msg"));
	}

	@Test
	public void parseSample2() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:35.002181Z [ha_traffic] [23.1.3.10] "
				+ "2013-05-28 09:56:35,localhost-M,0,0,0,0,1"));

		assertEquals("ha_traffic", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:35", m.get("event_at"));
		assertEquals("localhost-M,0,0,0,0,1", m.get("msg"));
	}

	@Test
	public void parseSample3() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:35.001997Z [fw6_traffic] [23.1.3.10] "
				+ "2013-05-28 09:56:35,localhost-M,0,0,0,0,0,0"));

		assertEquals("fw6_traffic", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:35", m.get("event_at"));
		assertEquals("localhost-M,0,0,0,0,0,0", m.get("msg"));
	}

	@Test
	public void parseSample4() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:35.008528Z [mng_qos] [23.1.3.10] "
				+ "2013-05-28 09:56:35,localhost-M,TCP,tun0,0,0,0,0,0"));

		assertEquals("mng_qos", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:35", m.get("event_at"));
		assertEquals("localhost-M,TCP,tun0,0,0,0,0,0", m.get("msg"));
	}

	@Test
	public void parseSample5() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:00.112555Z [mng_daemon] [23.1.3.10] "
				+ "2013-05-28 09:56:00,localhost-M,secuicmlogd,0.0,6724,3152"));

		assertEquals("mng_daemon", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:00", m.get("event_at"));
		assertEquals("localhost-M,secuicmlogd,0.0,6724,3152", m.get("msg"));
	}

	@Test
	public void parseSample6() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:35.022091Z [mng_resource] [23.1.3.10] "
				+ "2013-05-28 09:56:35,localhost-M,63,2.3,24682592,21.1,1912537148,3.0"));

		assertEquals("mng_resource", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:35", m.get("event_at"));
		assertEquals("localhost-M,63,2.3,24682592,21.1,1912537148,3.0", m.get("msg"));
	}

	@Test
	public void parseSample7() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:30.583105Z [mng_oversubscription] [23.1.3.10] "
				+ "2013-05-28 09:56:30,localhost-M,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0"));

		assertEquals("mng_oversubscription", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:30", m.get("event_at"));
		assertEquals("localhost-M,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0", m.get("msg"));
	}

	@Test
	public void parseSample8() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:35.008512Z [mng_qos] [23.1.3.10] "
				+ "2013-05-28 09:56:35,localhost-M,20130105_Qos,tun0,0,0,0,0,0"));

		assertEquals("mng_qos", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:35", m.get("event_at"));
		assertEquals("localhost-M,20130105_Qos,tun0,0,0,0,0,0", m.get("msg"));
	}

	@Test
	public void parseSample9() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:00.112537Z [mng_daemon]"
				+ " [23.1.3.10] 2013-05-28 09:56:00,localhost-M,report_smrd,0.0,125160,10712"));

		assertEquals("mng_daemon", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:00", m.get("event_at"));
		assertEquals("localhost-M,report_smrd,0.0,125160,10712", m.get("msg"));
	}

	@Test
	public void parseSample10() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:00.112395Z [mng_daemon]"
				+ " [23.1.3.10] 2013-05-28 09:56:00,localhost-M,ssl_gate_fast,0.0,50968,2252"));

		assertEquals("mng_daemon", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:00", m.get("event_at"));
		assertEquals("localhost-M,ssl_gate_fast,0.0,50968,2252", m.get("msg"));
	}

	@Test
	public void parseSample11() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:40:26.255516Z [fw4_allow] [23.1.3.2] "
				+ "start_time=\"2013-05-28 09:40:15\" end_time=\"2013-05-28 09:40:19\" "
				+ "duration=4 machine_name=localhost-M fw_rule_id=Local Out nat_rule_id=Undefined "
				+ "src_ip=23.1.3.4 src_port=48057 dst_ip=11.4.9.152 dst_port=1194 protocol=TCP "
				+ "ingres_if=LOCAL packets_forward=1 packets_backward=1 bytes_forward=70 "
				+ "bytes_backward=64 fragment_info=  flag_record=SYN Only / RST [S:AR] terminate_reason=-"));

		assertEquals("fw4_allow", m.get("type"));
		assertEquals("23.1.3.2", m.get("device"));
		assertEquals("2013-05-28 09:40:15", m.get("start_time"));
		assertEquals("2013-05-28 09:40:19", m.get("end_time"));
		assertEquals(4, m.get("duration"));
		assertEquals("localhost-M", m.get("machine_name"));
		assertEquals("Local Out", m.get("fw_rule_id"));
		assertEquals("Undefined", m.get("nat_rule_id"));
		assertEquals("23.1.3.4", m.get("src_ip"));
		assertEquals(48057, m.get("src_port"));
		assertEquals("11.4.9.152", m.get("dst_ip"));
		assertEquals(1194, m.get("dst_port"));
		assertEquals("TCP", m.get("protocol"));
		assertEquals("LOCAL", m.get("ingres_if"));
		assertEquals(1, m.get("packets_forward"));
		assertEquals(1, m.get("packets_backward"));
		assertEquals(70, m.get("bytes_forward"));
		assertEquals(64, m.get("bytes_backward"));
		assertNull(m.get("fragment_info"));
		assertEquals("SYN Only / RST [S:AR]", m.get("flag_record"));
		assertNull(m.get("terminate_reason"));
	}

	@Test
	public void parseSample12() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:40:26.255351Z [fw4_allow] [23.1.3.2] "
				+ "start_time=\"2013-05-28 09:40:06\" end_time=\"2013-05-28 09:40:21\" "
				+ "duration=15 machine_name=localhost-M fw_rule_id=55 nat_rule_id=Undefined "
				+ "src_ip=11.4.9.69 src_port=38826 dst_ip=23.1.3.2 dst_port=10003 protocol=TCP "
				+ "ingres_if=INT packets_forward=10 packets_backward=7 bytes_forward=1108 "
				+ "bytes_backward=1648 fragment_info=  flag_record=3Way OK / FIN2 [SAF:SAF] terminate_reason=-"));

		assertEquals("fw4_allow", m.get("type"));
		assertEquals("23.1.3.2", m.get("device"));
		assertEquals("2013-05-28 09:40:06", m.get("start_time"));
		assertEquals("2013-05-28 09:40:21", m.get("end_time"));
		assertEquals(15, m.get("duration"));
		assertEquals("localhost-M", m.get("machine_name"));
		assertEquals("55", m.get("fw_rule_id"));
		assertEquals("Undefined", m.get("nat_rule_id"));
		assertEquals("11.4.9.69", m.get("src_ip"));
		assertEquals(38826, m.get("src_port"));
		assertEquals("23.1.3.2", m.get("dst_ip"));
		assertEquals(10003, m.get("dst_port"));
		assertEquals("TCP", m.get("protocol"));
		assertEquals("INT", m.get("ingres_if"));
		assertEquals(10, m.get("packets_forward"));
		assertEquals(7, m.get("packets_backward"));
		assertEquals(1108, m.get("bytes_forward"));
		assertEquals(1648, m.get("bytes_backward"));
		assertNull(m.get("fragment_info"));
		assertEquals("3Way OK / FIN2 [SAF:SAF]", m.get("flag_record"));
		assertNull(m.get("terminate_reason"));
	}

	@Test
	public void parseSample13() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:40:36.932214Z [fw4_allow] [23.1.3.2] "
				+ "start_time=\"2013-05-28 09:40:26\" end_time=\"2013-05-28 09:40:36\" duration=10 "
				+ "machine_name=localhost-M fw_rule_id=55 nat_rule_id=Undefined src_ip=11.4.9.69 src_port=- "
				+ "dst_ip=23.1.3.3 dst_port=8 protocol=ICMP ingres_if=INT packets_forward=1 "
				+ "packets_backward=0 bytes_forward=102 bytes_backward=0 fragment_info=  flag_record=- terminate_reason=-"));

		assertEquals("fw4_allow", m.get("type"));
		assertEquals("23.1.3.2", m.get("device"));
		assertEquals("2013-05-28 09:40:26", m.get("start_time"));
		assertEquals("2013-05-28 09:40:36", m.get("end_time"));
		assertEquals(10, m.get("duration"));
		assertEquals("localhost-M", m.get("machine_name"));
		assertEquals("55", m.get("fw_rule_id"));
		assertEquals("Undefined", m.get("nat_rule_id"));
		assertEquals("11.4.9.69", m.get("src_ip"));
		assertNull(m.get("src_port"));
		assertEquals("23.1.3.3", m.get("dst_ip"));
		assertEquals(8, m.get("dst_port"));
		assertEquals("ICMP", m.get("protocol"));
		assertEquals("INT", m.get("ingres_if"));
		assertEquals(1, m.get("packets_forward"));
		assertEquals(0, m.get("packets_backward"));
		assertEquals(102, m.get("bytes_forward"));
		assertEquals(0, m.get("bytes_backward"));
		assertNull(m.get("fragment_info"));
		assertNull(m.get("flag_record"));
		assertNull(m.get("terminate_reason"));
	}

	@Test
	public void parseSample14() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:35.001947Z [app_cnt_urlblock] [23.1.3.10] "
				+ "2013-05-28 09:56:35,localhost-M,0"));

		assertEquals("app_cnt_urlblock", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:35", m.get("event_at"));
		assertEquals("localhost-M,0", m.get("msg"));
	}

	@Test
	public void parseSample15() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:06.264569Z [sslvpn3_cnt_tunnel] [23.1.3.10] "
				+ "2013-05-28 09:56:06,localhost-M,0,0"));

		assertEquals("sslvpn3_cnt_tunnel", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:06", m.get("event_at"));
		assertEquals("localhost-M,0,0", m.get("msg"));
	}

	@Test
	public void parseSample16() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:06.264542Z [sslvpn3_cnt_traffic] [23.1.3.10] "
				+ "2013-05-28 09:56:06,localhost-M,0,0,0"));

		assertEquals("sslvpn3_cnt_traffic", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:06", m.get("event_at"));
		assertEquals("localhost-M,0,0,0", m.get("msg"));
	}

	@Test
	public void parseSample17() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:35.002147Z [app_cnt_antivirus] [23.1.3.10] "
				+ "2013-05-28 09:56:35,localhost-M,0,0,0,FTP,ToClient"));

		assertEquals("app_cnt_antivirus", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:35", m.get("event_at"));
		assertEquals("localhost-M,0,0,0,FTP,ToClient", m.get("msg"));
	}

	@Test
	public void parseSample18() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:35.027295Z [mng_if_traffic] [23.1.3.10] "
				+ "2013-05-28 09:56:35,localhost-M,eth2,OFF,0,0,0,0"));

		assertEquals("mng_if_traffic", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:35", m.get("event_at"));
		assertEquals("localhost-M,eth2,OFF,0,0,0,0", m.get("msg"));
	}

	@Test
	public void parseSample19() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:35.001980Z [app_cnt_antispam] [23.1.3.10] "
				+ "2013-05-28 09:56:35,localhost-M,0,0,0,0,0,SMTP,Inbound"));

		assertEquals("app_cnt_antispam", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:35", m.get("event_at"));
		assertEquals("localhost-M,0,0,0,0,0,SMTP,Inbound", m.get("msg"));
	}

	@Test
	public void parseSample20() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:30.924778Z [app_cnt_antivirus] [23.1.3.10] "
				+ "2013-05-28 09:56:30,localhost-M,0,0,0,NATEON,Bi-direction"));

		assertEquals("app_cnt_antivirus", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:30", m.get("event_at"));
		assertEquals("localhost-M,0,0,0,NATEON,Bi-direction", m.get("msg"));
	}

	@Test
	public void parseSample21() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:32.510417Z [ips_ddos_traffic] [23.1.3.10] "
				+ "2013-05-28 09:56:32,localhost-M,0.0,0.0,0.0,0.0,0.1,0.0,260.8,0.0"));

		assertEquals("ips_ddos_traffic", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:32", m.get("event_at"));
		assertEquals("localhost-M,0.0,0.0,0.0,0.0,0.1,0.0,260.8,0.0", m.get("msg"));
	}

	@Test
	public void parseSample22() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:35.002029Z [app_cnt_webclient_limit] [23.1.3.10] "
				+ "2013-05-28 09:56:35,localhost-M,0,0"));

		assertEquals("app_cnt_webclient_limit", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:35", m.get("event_at"));
		assertEquals("localhost-M,0,0", m.get("msg"));
	}

	@Test
	public void parseSample23() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:32.068001Z [vpn_cnt_tunnel_use] [23.1.3.10] "
				+ "2013-05-28 09:56:32,localhost-M,0,0,0"));

		assertEquals("vpn_cnt_tunnel_use", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:32", m.get("event_at"));
		assertEquals("localhost-M,0,0,0", m.get("msg"));
	}

	@Test
	public void parseSample24() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p.parse(line("<14>1 2013-05-28T00:56:32.509973Z [app_cnt_webserver_protect] [23.1.3.10] "
				+ "2013-05-28 09:56:32,localhost-M,0,0,0,0"));

		assertEquals("app_cnt_webserver_protect", m.get("type"));
		assertEquals("23.1.3.10", m.get("device"));
		assertEquals("2013-05-28 09:56:32", m.get("event_at"));
		assertEquals("localhost-M,0,0,0,0", m.get("msg"));
	}

	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
