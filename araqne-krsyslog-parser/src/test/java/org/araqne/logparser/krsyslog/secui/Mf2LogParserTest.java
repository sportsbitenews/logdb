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
package org.araqne.logparser.krsyslog.secui;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.araqne.logparser.krsyslog.secui.Mf2LogParser;
import org.araqne.logparser.krsyslog.secui.Mf2LogParser.Mode;
import org.junit.Test;

/**
 * @since 1.9.2
 * @author mindori
 * 
 *         rule of function signature: [(CSV|TSV|WELF)]Test[log number(e.g. 1_4
 *         means 1.4 HA Status Log)] see also: TEC-08-11_001_SECUI MF2 Syslog
 *         전송포맷 V2.0.pdf
 * 
 */
public class Mf2LogParserTest {
	@Test
	public void testBrokenLogParse() {
		String line = "<190>1 2015-02-17T04:35:41.384736Z [ips_ddos_incident] [210.223.182.1] 2015-02-17 13:35:37,2015-02-17 13:35:37,DaeYang,#14715(HTTP Authorization Login Brute Force Attempt-1/3(count 10, seconds 1)),Low,#0(IPS_DDOS占쏙옙占?,1,1518,detect,10425309775129";

		Mf2LogParser p = new Mf2LogParser(Mode.CSV);	
		Map<String, Object> m = p.parse(line(line));
		System.out.println(m);

	}
	
	@Test
	public void testBrokenLogParse2() {
		String line = "<190>1 2015-02-16T09:17:33.113629Z [vpn_act_ike] [210.223.182.1] 2015-02-16 18:17:33,DaeYang,-,0.0.0.0,-,SUCCESS,DEBUG,[exchange_free_aux:L2372] exchange (ptr=0x1c27c80, name=nil, remote_addr=0.0.0.0, phase=1) released.";

		Mf2LogParser p = new Mf2LogParser(Mode.CSV);	
		Map<String, Object> m = p.parse(line(line));
		System.out.println(m);

	}
	
	@Test
	public void csvTest1_1() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[ha_event] [...] 2015-01-14 08:56:45,localhost,mindori event,ha event msg")));
		assertEquals("ha_event", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("mindori event", m.get("event_name"));
		assertEquals("ha event msg", m.get("msg"));
	}

	@Test
	public void csvTest1_2() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[audit] [...] 2015-01-14 08:56:45,localhost,mindori,127.0.0.1,5,C:\\,COMMAND,OK,-,-")));
		assertEquals("audit", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("mindori", m.get("admin_id"));
		assertEquals("127.0.0.1", m.get("admin_ip"));
		assertEquals("5", m.get("admin_level"));
		assertEquals("C:\\", m.get("menu_path"));
		assertEquals("COMMAND", m.get("command"));
		assertEquals("OK", m.get("result"));
		assertEquals("-", m.get("fail_reason"));
		assertEquals("-", m.get("difference"));
	}

	@Test
	public void csvTest1_3() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[ha_traffic] [...] 2015-01-14 08:56:45,localhost,5,5,1024,2048,1")));
		assertEquals("ha_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("5", m.get("tx_packets"));
		assertEquals("5", m.get("rx_packets"));
		assertEquals("1024", m.get("tx_bytes"));
		assertEquals("2048", m.get("rx_bytes"));
		assertEquals("1", m.get("traffictype"));
	}

	@Test
	public void csvTest1_4() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[ha_status] [...] 2015-01-14 08:56:45,eediom_member,mindori,5,777")));
		assertEquals("ha_status", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("eediom_member", m.get("members"));
		assertEquals("mindori", m.get("member_name"));
		assertEquals("5", m.get("priority"));
		assertEquals("777", m.get("member_status"));
	}

	@Test
	public void csvTest1_5() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[mng_iap_interworking] [...] 2015-01-14 08:56:45,localhost,192.168.0.1,127.0.0.1,55,210.55.6.123,80,TCP,1 day,-")));
		assertEquals("mng_iap_interworking", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("192.168.0.1", m.get("sensor_ip"));
		assertEquals("127.0.0.1", m.get("attacker_ip"));
		assertEquals("55", m.get("attacker_port"));
		assertEquals("210.55.6.123", m.get("victim_ip"));
		assertEquals("80", m.get("victim_port"));
		assertEquals("TCP", m.get("protocol"));
		assertEquals("1 day", m.get("block_period"));
		assertEquals("-", m.get("msg"));
	}

	@Test
	public void csvTest1_6() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[mng_blacklist] [...] 2015-01-14 08:56:45,localhost,192.168.0.1,80,210.50.7.102,55,TCP,1 day,3,4096,5,-")));
		assertEquals("mng_blacklist", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("192.168.0.1", m.get("src_ip"));
		assertEquals("80", m.get("src_port"));
		assertEquals("210.50.7.102", m.get("dst_ip"));
		assertEquals("55", m.get("dst_port"));
		assertEquals("TCP", m.get("protocol"));
		assertEquals("1 day", m.get("block_period"));
		assertEquals("3", m.get("packets"));
		assertEquals("4096", m.get("bytes"));
		assertEquals("5", m.get("type"));
		assertEquals("-", m.get("reason"));
	}

	@Test
	public void csvTest1_7() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[mng_blacklist_ipv6] [...] 2015-01-14 08:56:45,localhost,192.168.0.1,80,210.50.7.102,55,TCP,1 day,3,4096,5,-")));
		assertEquals("mng_blacklist_ipv6", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("192.168.0.1", m.get("src_ip"));
		assertEquals("80", m.get("src_port"));
		assertEquals("210.50.7.102", m.get("dst_ip"));
		assertEquals("55", m.get("dst_port"));
		assertEquals("TCP", m.get("protocol"));
		assertEquals("1 day", m.get("block_period"));
		assertEquals("3", m.get("packets"));
		assertEquals("4096", m.get("bytes"));
		assertEquals("5", m.get("type"));
		assertEquals("-", m.get("reason"));
	}

	@Test
	public void csvTest1_8() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[mng_line] [...] 2015-01-14 08:56:45,localhost,My interface,My msg!")));
		assertEquals("mng_line", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("My interface", m.get("interface"));
		assertEquals("My msg!", m.get("msg"));
	}

	@Test
	public void csvTest1_9_1() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[mng_resource] [...] 2015-01-14 08:56:45,localhost,30,2.8,3973452,26.4,230041436,6.4")));
		assertEquals("mng_resource", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("30", m.get("cpu_cores"));
		assertEquals("2.8", m.get("cpu_usages"));
		assertEquals("3973452", m.get("memory_capacity"));
		assertEquals("26.4", m.get("memory_usages"));
		assertEquals("230041436", m.get("disk_capacity"));
		assertEquals("6.4", m.get("disk_usages"));
	}

	@Test
	public void csvTest1_9_2() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[mng_resource] [...] 2015-01-14 09:21:50,KNIAFWIN2,20,1.6,8062584,21.0,470397848,0.0")));
		assertEquals("mng_resource", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 09:21:50", m.get("time"));
		assertEquals("KNIAFWIN2", m.get("machine_name"));
		assertEquals("20", m.get("cpu_cores"));
		assertEquals("1.6", m.get("cpu_usages"));
		assertEquals("8062584", m.get("memory_capacity"));
		assertEquals("21.0", m.get("memory_usages"));
		assertEquals("470397848", m.get("disk_capacity"));
		assertEquals("0.0", m.get("disk_usages"));
	}

	@Test
	public void csvTest1_9_3() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[mng_resource] [...] 2015-01-14 09:21:50,KNIAFWIN1,15,2.2,8062584,23.4,470397848,80.0")));
		assertEquals("mng_resource", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 09:21:50", m.get("time"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("15", m.get("cpu_cores"));
		assertEquals("2.2", m.get("cpu_usages"));
		assertEquals("8062584", m.get("memory_capacity"));
		assertEquals("23.4", m.get("memory_usages"));
		assertEquals("470397848", m.get("disk_capacity"));
		assertEquals("80.0", m.get("disk_usages"));
	}

	@Test
	public void csvTest1_9_4() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[mng_resource] [...] 2015-01-14 08:56:45,mindori,30,2.8234234234234,3973452,26.4, ,16.4")));
		assertEquals("mng_resource", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("30", m.get("cpu_cores"));
		assertEquals("2.8234234234234", m.get("cpu_usages"));
		assertEquals("3973452", m.get("memory_capacity"));
		assertEquals("26.4", m.get("memory_usages"));
		assertEquals(" ", m.get("disk_capacity"));
		assertEquals("16.4", m.get("disk_usages"));
	}

	@Test
	public void csvTest1_10() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[mng_daemon] [...] 2015-01-14 08:56:45,mindori,foo,2.222,50,26.4")));
		assertEquals("mng_daemon", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("foo", m.get("daemon_name"));
		assertEquals("2.222", m.get("cpu_usages"));
		assertEquals("50", m.get("virtual_memmory_usages"));
		assertEquals("26.4", m.get("real_memory_usages"));
	}

	@Test
	public void csvTest1_11() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[mng_if_traffic] [...] 2015-01-14 08:56:45,mindori,mindori_interface,GOOD,55,50,26400,25000")));
		assertEquals("mng_if_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("mindori_interface", m.get("interface"));
		assertEquals("GOOD", m.get("link_status"));
		assertEquals("55", m.get("rx_frames"));
		assertEquals("50", m.get("tx_frames"));
		assertEquals("26400", m.get("rx_bytes"));
		assertEquals("25000", m.get("tx_bytes"));
	}

	@Test
	public void csvTest1_12() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[mng_oversubscription] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5.5,66,77,8")));
		assertEquals("mng_oversubscription", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("incoming_bypass_pps"));
		assertEquals("2", m.get("incoming_bypass_bps"));
		assertEquals("3", m.get("incoming_drop_pps"));
		assertEquals("4", m.get("incoming_drop_bps"));
		assertEquals("5.5", m.get("outgoing_bypass_pps"));
		assertEquals("66", m.get("outgoing_bypass_bps"));
		assertEquals("77", m.get("outgoing_drop_pps"));
		assertEquals("8", m.get("outgoing_drop_bps"));
	}

	@Test
	public void csvTest1_13() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[mng_qos] [...] 2015-01-14 08:56:45,mindori,mindori_q,-,30000,4,55,6,77")));
		assertEquals("mng_qos", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("mindori_q", m.get("queue_name"));
		assertEquals("-", m.get("interface"));
		assertEquals("30000", m.get("use_bandwidth"));
		assertEquals("4", m.get("allow_packets"));
		assertEquals("55", m.get("allow_rate"));
		assertEquals("6", m.get("loss_packets"));
		assertEquals("77", m.get("loss_rate"));
	}

	@Test
	public void csvTest1_14() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[mng_fqdn_object_management] [...] 2015-01-14 08:56:45,mindori,mindori_q,-, ")));
		assertEquals("mng_fqdn_object_management", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("mindori_q", m.get("object_name"));
		assertEquals("-", m.get("before"));
		assertEquals(" ", m.get("after"));
	}

	@Test
	public void csvTest1_15() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[mng_user_object_management] [...] 2015-01-14 08:56:45,mindori,mindori_q,-, ")));
		assertEquals("mng_user_object_management", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("mindori_q", m.get("user_name"));
		assertEquals("-", m.get("before"));
		assertEquals(" ", m.get("after"));
	}

	@Test
	public void csvTest2_1_1() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw4_allow] [...] 2015-01-14 09:21:41,2015-01-14 09:21:46,5,KNIAFWIN1,93,Undefined,42.1.52.79,53363,172.18.1.20,33008,TCP,EXT,4,5,366,704, ,3Way OK / FIN2 [SAF:SAF],-")));
		assertEquals("fw4_allow", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 09:21:41", m.get("start_time"));
		assertEquals("2015-01-14 09:21:46", m.get("end_time"));
		assertEquals("5", m.get("duration"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("93", m.get("fw_rule_id"));
		assertEquals("Undefined", m.get("nat_rule_id"));
		assertEquals("42.1.52.79", m.get("src_ip"));
		assertEquals("53363", m.get("src_port"));
		assertEquals("172.18.1.20", m.get("dst_ip"));
		assertEquals("33008", m.get("dst_port"));
		assertEquals("TCP", m.get("protocol"));
		assertEquals("EXT", m.get("ingres_if"));
		assertEquals("4", m.get("tx_packets"));
		assertEquals("5", m.get("rx_packets"));
		assertEquals("366", m.get("tx_bytes"));
		assertEquals("704", m.get("rx_bytes"));
		assertEquals(" ", m.get("fragment_info"));
		assertEquals("3Way OK / FIN2 [SAF:SAF]", m.get("flag_record"));
		assertEquals("-", m.get("terminate_reason"));
	}

	@Test
	public void csvTest2_1_2() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw4_allow] [...] 2015-01-14 09:21:43,2015-01-14 09:21:50,7,KNIAFWIN1,93,Undefined,42.1.52.81,50346,172.18.1.20,38108,TCP,EXT,6,7,494,3729, ,3Way OK / FIN2 [SAF:SAF],-")));
		assertEquals("fw4_allow", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 09:21:43", m.get("start_time"));
		assertEquals("2015-01-14 09:21:50", m.get("end_time"));
		assertEquals("7", m.get("duration"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("93", m.get("fw_rule_id"));
		assertEquals("Undefined", m.get("nat_rule_id"));
		assertEquals("42.1.52.81", m.get("src_ip"));
		assertEquals("50346", m.get("src_port"));
		assertEquals("172.18.1.20", m.get("dst_ip"));
		assertEquals("38108", m.get("dst_port"));
		assertEquals("TCP", m.get("protocol"));
		assertEquals("EXT", m.get("ingres_if"));
		assertEquals("6", m.get("tx_packets"));
		assertEquals("7", m.get("rx_packets"));
		assertEquals("494", m.get("tx_bytes"));
		assertEquals("3729", m.get("rx_bytes"));
		assertEquals(" ", m.get("fragment_info"));
		assertEquals("3Way OK / FIN2 [SAF:SAF]", m.get("flag_record"));
		assertEquals("-", m.get("terminate_reason"));
	}

	@Test
	public void csvTest2_1_3() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw4_allow] [...] 2015-01-14 09:21:14,2015-01-14 09:21:45,31,KNIAFWIN1,Local Out,Undefined,172.100.11.4,-,172.16.33.5,3,ICMP,LOCAL,6,0,580,0, ,-,-")));
		assertEquals("fw4_allow", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 09:21:14", m.get("start_time"));
		assertEquals("2015-01-14 09:21:45", m.get("end_time"));
		assertEquals("31", m.get("duration"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("Local Out", m.get("fw_rule_id"));
		assertEquals("Undefined", m.get("nat_rule_id"));
		assertEquals("172.100.11.4", m.get("src_ip"));
		assertEquals("-", m.get("src_port"));
		assertEquals("172.16.33.5", m.get("dst_ip"));
		assertEquals("3", m.get("dst_port"));
		assertEquals("ICMP", m.get("protocol"));
		assertEquals("LOCAL", m.get("ingres_if"));
		assertEquals("6", m.get("tx_packets"));
		assertEquals("0", m.get("rx_packets"));
		assertEquals("580", m.get("tx_bytes"));
		assertEquals("0", m.get("rx_bytes"));
		assertEquals(" ", m.get("fragment_info"));
		assertEquals("-", m.get("flag_record"));
		assertEquals("-", m.get("terminate_reason"));
	}

	@Test
	public void csvTest2_1_4() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw4_allow] [...] 2015-01-14 09:21:37,2015-01-14 09:21:42,5,KNIAFWIN1,93,Undefined,42.1.52.81,50282,172.18.1.20,38108,TCP,EXT,6,7,494,3729, ,3Way OK / FIN2 [SAF:SAF],-")));
		assertEquals("fw4_allow", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 09:21:37", m.get("start_time"));
		assertEquals("2015-01-14 09:21:42", m.get("end_time"));
		assertEquals("5", m.get("duration"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("93", m.get("fw_rule_id"));
		assertEquals("Undefined", m.get("nat_rule_id"));
		assertEquals("42.1.52.81", m.get("src_ip"));
		assertEquals("50282", m.get("src_port"));
		assertEquals("172.18.1.20", m.get("dst_ip"));
		assertEquals("38108", m.get("dst_port"));
		assertEquals("TCP", m.get("protocol"));
		assertEquals("EXT", m.get("ingres_if"));
		assertEquals("6", m.get("tx_packets"));
		assertEquals("7", m.get("rx_packets"));
		assertEquals("494", m.get("tx_bytes"));
		assertEquals("3729", m.get("rx_bytes"));
		assertEquals(" ", m.get("fragment_info"));
		assertEquals("3Way OK / FIN2 [SAF:SAF]", m.get("flag_record"));
		assertEquals("-", m.get("terminate_reason"));
	}

	@Test
	public void csvTest2_2_1() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw4_deny] [...] 2015-01-14 09:21:52,2015-01-14 09:21:52,0,KNIAFWIN1,0,Undefined,0.0.0.0,68,255.255.255.255,67,UDP,EXT,1,335, ,-,Deny by Deny Rule")));
		assertEquals("fw4_deny", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 09:21:52", m.get("start_time"));
		assertEquals("2015-01-14 09:21:52", m.get("end_time"));
		assertEquals("0", m.get("duration"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("Undefined", m.get("nat_rule_id"));
		assertEquals("0.0.0.0", m.get("src_ip"));
		assertEquals("68", m.get("src_port"));
		assertEquals("255.255.255.255", m.get("dst_ip"));
		assertEquals("67", m.get("dst_port"));
		assertEquals("UDP", m.get("protocol"));
		assertEquals("EXT", m.get("ingres_if"));
		assertEquals("1", m.get("packets"));
		assertEquals("335", m.get("bytes"));
		assertEquals(" ", m.get("fragment_info"));
		assertEquals("-", m.get("flag_record"));
		assertEquals("Deny by Deny Rule", m.get("terminate_reason"));
	}

	@Test
	public void csvTest2_2_2() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw4_deny] [...] 2015-01-14 09:24:02,2015-01-14 09:24:02,0,master.fw.com,1,Undefined,0.0.0.0,68,255.255.255.255,67,UDP,EXT,1,335, ,-,Deny by Deny Rule")));
		assertEquals("fw4_deny", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 09:24:02", m.get("start_time"));
		assertEquals("2015-01-14 09:24:02", m.get("end_time"));
		assertEquals("0", m.get("duration"));
		assertEquals("master.fw.com", m.get("machine_name"));
		assertEquals("1", m.get("fw_rule_id"));
		assertEquals("Undefined", m.get("nat_rule_id"));
		assertEquals("0.0.0.0", m.get("src_ip"));
		assertEquals("68", m.get("src_port"));
		assertEquals("255.255.255.255", m.get("dst_ip"));
		assertEquals("67", m.get("dst_port"));
		assertEquals("UDP", m.get("protocol"));
		assertEquals("EXT", m.get("ingres_if"));
		assertEquals("1", m.get("packets"));
		assertEquals("335", m.get("bytes"));
		assertEquals(" ", m.get("fragment_info"));
		assertEquals("-", m.get("flag_record"));
		assertEquals("Deny by Deny Rule", m.get("terminate_reason"));
	}

	@Test
	public void csvTest2_2_3() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw4_deny] [...] 2015-01-14 08:56:46,2015-01-14 08:56:46,0,localhost,0,Undefined,172.100.40.2,-,224.0.0.18,-,VRRP,EXT,1,70, ,-,Deny by Deny Rule")));
		assertEquals("fw4_deny", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:46", m.get("start_time"));
		assertEquals("2015-01-14 08:56:46", m.get("end_time"));
		assertEquals("0", m.get("duration"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("Undefined", m.get("nat_rule_id"));
		assertEquals("172.100.40.2", m.get("src_ip"));
		assertEquals("-", m.get("src_port"));
		assertEquals("224.0.0.18", m.get("dst_ip"));
		assertEquals("-", m.get("dst_port"));
		assertEquals("VRRP", m.get("protocol"));
		assertEquals("EXT", m.get("ingres_if"));
		assertEquals("1", m.get("packets"));
		assertEquals("70", m.get("bytes"));
		assertEquals(" ", m.get("fragment_info"));
		assertEquals("-", m.get("flag_record"));
		assertEquals("Deny by Deny Rule", m.get("terminate_reason"));
	}

	@Test
	public void csvTest2_2_4() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw4_deny] [...] 2015-01-14 09:21:50,2015-01-14 09:21:50,0,KNIAFWIN1,0,Undefined,172.31.0.9,137,172.31.255.255,137,UDP,EXT,1,96, ,-,Deny by Deny Rule")));
		assertEquals("fw4_deny", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 09:21:50", m.get("start_time"));
		assertEquals("2015-01-14 09:21:50", m.get("end_time"));
		assertEquals("0", m.get("duration"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("Undefined", m.get("nat_rule_id"));
		assertEquals("172.31.0.9", m.get("src_ip"));
		assertEquals("137", m.get("src_port"));
		assertEquals("172.31.255.255", m.get("dst_ip"));
		assertEquals("137", m.get("dst_port"));
		assertEquals("UDP", m.get("protocol"));
		assertEquals("EXT", m.get("ingres_if"));
		assertEquals("1", m.get("packets"));
		assertEquals("96", m.get("bytes"));
		assertEquals(" ", m.get("fragment_info"));
		assertEquals("-", m.get("flag_record"));
		assertEquals("Deny by Deny Rule", m.get("terminate_reason"));
	}

	@Test
	public void csvTest2_3_1() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw6_allow] [...] 2015-01-14 09:21:50,2015-01-14 09:21:50,0,KNIAFWIN1,0,172.31.0.9,137,172.31.255.255,137,UDP,EXT,1,1,96,196,96, ,-,-")));
		assertEquals("fw6_allow", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 09:21:50", m.get("start_time"));
		assertEquals("2015-01-14 09:21:50", m.get("end_time"));
		assertEquals("0", m.get("duration"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("172.31.0.9", m.get("src_ip"));
		assertEquals("137", m.get("src_port"));
		assertEquals("172.31.255.255", m.get("dst_ip"));
		assertEquals("137", m.get("dst_port"));
		assertEquals("UDP", m.get("protocol"));
		assertEquals("EXT", m.get("ingres_if"));
		assertEquals("1", m.get("tx_packets"));
		assertEquals("1", m.get("rx_packets"));
		assertEquals("96", m.get("tx_bytes"));
		assertEquals("196", m.get("rx_bytes"));
		assertEquals("96", m.get("extented_header"));
		assertEquals(" ", m.get("fragment_info"));
		assertEquals("-", m.get("flag_record"));
		assertEquals("-", m.get("terminate_reason"));
	}

	@Test
	public void csvTest2_3_2() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw6_allow] [...] 2015-01-14 07:00:00,2015-01-14 07:00:00,0,LOCAL,0,172.30.10.204,1137,172.31.255.255,137,UDP,EXT,1,1,9346,196,96, ,-,-")));
		assertEquals("fw6_allow", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 07:00:00", m.get("start_time"));
		assertEquals("2015-01-14 07:00:00", m.get("end_time"));
		assertEquals("0", m.get("duration"));
		assertEquals("LOCAL", m.get("machine_name"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("172.30.10.204", m.get("src_ip"));
		assertEquals("1137", m.get("src_port"));
		assertEquals("172.31.255.255", m.get("dst_ip"));
		assertEquals("137", m.get("dst_port"));
		assertEquals("UDP", m.get("protocol"));
		assertEquals("EXT", m.get("ingres_if"));
		assertEquals("1", m.get("tx_packets"));
		assertEquals("1", m.get("rx_packets"));
		assertEquals("9346", m.get("tx_bytes"));
		assertEquals("196", m.get("rx_bytes"));
		assertEquals("96", m.get("extented_header"));
		assertEquals(" ", m.get("fragment_info"));
		assertEquals("-", m.get("flag_record"));
		assertEquals("-", m.get("terminate_reason"));
	}

	@Test
	public void csvTest2_3_3() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw6_allow] [...] 2015-01-30 09:21:50,2015-01-30 10:21:50,55,KNIAFWIN1,0,172.31.0.9,137,172.31.255.255,137,ICMP,EXT,1,1,96,196,96, ,-,-")));
		assertEquals("fw6_allow", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-30 09:21:50", m.get("start_time"));
		assertEquals("2015-01-30 10:21:50", m.get("end_time"));
		assertEquals("55", m.get("duration"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("172.31.0.9", m.get("src_ip"));
		assertEquals("137", m.get("src_port"));
		assertEquals("172.31.255.255", m.get("dst_ip"));
		assertEquals("137", m.get("dst_port"));
		assertEquals("ICMP", m.get("protocol"));
		assertEquals("EXT", m.get("ingres_if"));
		assertEquals("1", m.get("tx_packets"));
		assertEquals("1", m.get("rx_packets"));
		assertEquals("96", m.get("tx_bytes"));
		assertEquals("196", m.get("rx_bytes"));
		assertEquals("96", m.get("extented_header"));
		assertEquals(" ", m.get("fragment_info"));
		assertEquals("-", m.get("flag_record"));
		assertEquals("-", m.get("terminate_reason"));
	}

	@Test
	public void csvTest2_3_4() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw6_allow] [...] 2015-01-14 09:21:50,2015-01-14 09:21:50,0,KNIAFWIN1,0,172.31.0.9,137,172.31.255.255,137,UDP,EXT,1,1,0,0,0, ,-,-")));
		assertEquals("fw6_allow", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 09:21:50", m.get("start_time"));
		assertEquals("2015-01-14 09:21:50", m.get("end_time"));
		assertEquals("0", m.get("duration"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("172.31.0.9", m.get("src_ip"));
		assertEquals("137", m.get("src_port"));
		assertEquals("172.31.255.255", m.get("dst_ip"));
		assertEquals("137", m.get("dst_port"));
		assertEquals("UDP", m.get("protocol"));
		assertEquals("EXT", m.get("ingres_if"));
		assertEquals("1", m.get("tx_packets"));
		assertEquals("1", m.get("rx_packets"));
		assertEquals("0", m.get("tx_bytes"));
		assertEquals("0", m.get("rx_bytes"));
		assertEquals("0", m.get("extented_header"));
		assertEquals(" ", m.get("fragment_info"));
		assertEquals("-", m.get("flag_record"));
		assertEquals("-", m.get("terminate_reason"));
	}

	@Test
	public void csvTest2_4_1() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw6_deny] [...] 2015-01-14 09:21:50,2015-01-14 09:21:50,0,KNIAFWIN1,0,172.31.0.9,137,172.31.255.255,137,UDP,EXT,1,96, ,-,-,Deny by Deny Rule")));
		assertEquals("fw6_deny", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 09:21:50", m.get("start_time"));
		assertEquals("2015-01-14 09:21:50", m.get("end_time"));
		assertEquals("0", m.get("duration"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("172.31.0.9", m.get("src_ip"));
		assertEquals("137", m.get("src_port"));
		assertEquals("172.31.255.255", m.get("dst_ip"));
		assertEquals("137", m.get("dst_port"));
		assertEquals("UDP", m.get("protocol"));
		assertEquals("EXT", m.get("ingres_if"));
		assertEquals("1", m.get("packets"));
		assertEquals("96", m.get("bytes"));
		assertEquals(" ", m.get("extented_header"));
		assertEquals("-", m.get("fragment_info"));
		assertEquals("-", m.get("flag_record"));
		assertEquals("Deny by Deny Rule", m.get("terminate_reason"));
	}

	@Test
	public void csvTest2_4_2() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw6_deny] [...] 2015-01-14 09:21:50,2015-01-14 09:21:50,0,KNIAFWIN1,0,172.31.0.9,137,172.31.255.255,137,UDP,EXT,1,96,EXTENTED_HEADER_SAMPLE,-,-,Deny by mindori.")));
		assertEquals("fw6_deny", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 09:21:50", m.get("start_time"));
		assertEquals("2015-01-14 09:21:50", m.get("end_time"));
		assertEquals("0", m.get("duration"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("172.31.0.9", m.get("src_ip"));
		assertEquals("137", m.get("src_port"));
		assertEquals("172.31.255.255", m.get("dst_ip"));
		assertEquals("137", m.get("dst_port"));
		assertEquals("UDP", m.get("protocol"));
		assertEquals("EXT", m.get("ingres_if"));
		assertEquals("1", m.get("packets"));
		assertEquals("96", m.get("bytes"));
		assertEquals("EXTENTED_HEADER_SAMPLE", m.get("extented_header"));
		assertEquals("-", m.get("fragment_info"));
		assertEquals("-", m.get("flag_record"));
		assertEquals("Deny by mindori.", m.get("terminate_reason"));
	}

	@Test
	public void csvTest2_4_3() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw6_deny] [192.168.0.1] 09:21:50,09:21:50,0,KNIAFWIN1,0,172.31.0.9,137,172.31.255.255,137,UDP,EXT,1,96,EXTENTED_HEADER_SAMPLE,-,-,Deny by mindori.")));
		assertEquals("fw6_deny", m.get("log_type"));
		assertEquals("192.168.0.1", m.get("from_ip"));
		assertEquals("09:21:50", m.get("start_time"));
		assertEquals("09:21:50", m.get("end_time"));
		assertEquals("0", m.get("duration"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("172.31.0.9", m.get("src_ip"));
		assertEquals("137", m.get("src_port"));
		assertEquals("172.31.255.255", m.get("dst_ip"));
		assertEquals("137", m.get("dst_port"));
		assertEquals("UDP", m.get("protocol"));
		assertEquals("EXT", m.get("ingres_if"));
		assertEquals("1", m.get("packets"));
		assertEquals("96", m.get("bytes"));
		assertEquals("EXTENTED_HEADER_SAMPLE", m.get("extented_header"));
		assertEquals("-", m.get("fragment_info"));
		assertEquals("-", m.get("flag_record"));
		assertEquals("Deny by mindori.", m.get("terminate_reason"));
	}

	@Test
	public void csvTest2_4_4() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw6_deny] [...] 09:21:50,09:21:50,0,KNIAFWIN1,0,172.31.0.9,137,172.31.255.255,137,TCP,EXT,1,96,EXTENTED_HEADER_SAMPLE,INFO,-,Deny by mindori.")));
		assertEquals("fw6_deny", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("09:21:50", m.get("start_time"));
		assertEquals("09:21:50", m.get("end_time"));
		assertEquals("0", m.get("duration"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("172.31.0.9", m.get("src_ip"));
		assertEquals("137", m.get("src_port"));
		assertEquals("172.31.255.255", m.get("dst_ip"));
		assertEquals("137", m.get("dst_port"));
		assertEquals("TCP", m.get("protocol"));
		assertEquals("EXT", m.get("ingres_if"));
		assertEquals("1", m.get("packets"));
		assertEquals("96", m.get("bytes"));
		assertEquals("EXTENTED_HEADER_SAMPLE", m.get("extented_header"));
		assertEquals("INFO", m.get("fragment_info"));
		assertEquals("-", m.get("flag_record"));
		assertEquals("Deny by mindori.", m.get("terminate_reason"));
	}

	@Test
	public void csvTest2_5_1() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[nat_session] [...] 09:21:50,09:21:50,KNIAFWIN1,0,0,1,172.31.0.9,137,172.31.255.255,137,TCP,220.220.1.11,210.23.55.42,1000,2000,3,4096")));
		assertEquals("nat_session", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("09:21:50", m.get("start_time"));
		assertEquals("09:21:50", m.get("end_time"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("0", m.get("nat_rule_id"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("1", m.get("applied_if"));
		assertEquals("172.31.0.9", m.get("src_ip"));
		assertEquals("137", m.get("src_port"));
		assertEquals("172.31.255.255", m.get("dst_ip"));
		assertEquals("137", m.get("dst_port"));
		assertEquals("TCP", m.get("protocol"));
		assertEquals("220.220.1.11", m.get("src_ip_nat"));
		assertEquals("210.23.55.42", m.get("dst_ip_nat"));
		assertEquals("1000", m.get("src_port_nat"));
		assertEquals("2000", m.get("dst_port_nat"));
		assertEquals("3", m.get("packets"));
		assertEquals("4096", m.get("bytes"));
	}

	@Test
	public void csvTest2_5_2() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[nat_session] [...] 2015-01-14 09:21:50,2015-01-14 09:21:50,KNIAFWIN1,0,0,1,172.31.0.9,137,172.31.255.255,137,TCP,220.220.1.11,210.23.55.42,1000,2000,3,4096")));
		assertEquals("nat_session", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 09:21:50", m.get("start_time"));
		assertEquals("2015-01-14 09:21:50", m.get("end_time"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("0", m.get("nat_rule_id"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("1", m.get("applied_if"));
		assertEquals("172.31.0.9", m.get("src_ip"));
		assertEquals("137", m.get("src_port"));
		assertEquals("172.31.255.255", m.get("dst_ip"));
		assertEquals("137", m.get("dst_port"));
		assertEquals("TCP", m.get("protocol"));
		assertEquals("220.220.1.11", m.get("src_ip_nat"));
		assertEquals("210.23.55.42", m.get("dst_ip_nat"));
		assertEquals("1000", m.get("src_port_nat"));
		assertEquals("2000", m.get("dst_port_nat"));
		assertEquals("3", m.get("packets"));
		assertEquals("4096", m.get("bytes"));
	}

	@Test
	public void csvTest2_5_3() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[nat_session] [192.168.0.1] 2015-01-14 09:21:50,2015-01-14 09:21:50,KNIAFWIN1,50,0,1,172.31.0.9,137,172.31.255.255,137,TCP,220.220.1.11,210.23.55.42,1000,2000,3,4096")));
		assertEquals("nat_session", m.get("log_type"));
		assertEquals("192.168.0.1", m.get("from_ip"));
		assertEquals("2015-01-14 09:21:50", m.get("start_time"));
		assertEquals("2015-01-14 09:21:50", m.get("end_time"));
		assertEquals("KNIAFWIN1", m.get("machine_name"));
		assertEquals("50", m.get("nat_rule_id"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("1", m.get("applied_if"));
		assertEquals("172.31.0.9", m.get("src_ip"));
		assertEquals("137", m.get("src_port"));
		assertEquals("172.31.255.255", m.get("dst_ip"));
		assertEquals("137", m.get("dst_port"));
		assertEquals("TCP", m.get("protocol"));
		assertEquals("220.220.1.11", m.get("src_ip_nat"));
		assertEquals("210.23.55.42", m.get("dst_ip_nat"));
		assertEquals("1000", m.get("src_port_nat"));
		assertEquals("2000", m.get("dst_port_nat"));
		assertEquals("3", m.get("packets"));
		assertEquals("4096", m.get("bytes"));
	}

	@Test
	public void csvTest2_5_4() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[nat_session] [...] 2015-01-14 09:21:50,2015-01-14 09:21:50,LOCAL,50,0,1,172.31.0.9,137,172.31.255.255,137,UDP,220.220.1.11,210.23.55.42,1000,2000,3,4096")));
		assertEquals("nat_session", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 09:21:50", m.get("start_time"));
		assertEquals("2015-01-14 09:21:50", m.get("end_time"));
		assertEquals("LOCAL", m.get("machine_name"));
		assertEquals("50", m.get("nat_rule_id"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("1", m.get("applied_if"));
		assertEquals("172.31.0.9", m.get("src_ip"));
		assertEquals("137", m.get("src_port"));
		assertEquals("172.31.255.255", m.get("dst_ip"));
		assertEquals("137", m.get("dst_port"));
		assertEquals("UDP", m.get("protocol"));
		assertEquals("220.220.1.11", m.get("src_ip_nat"));
		assertEquals("210.23.55.42", m.get("dst_ip_nat"));
		assertEquals("1000", m.get("src_port_nat"));
		assertEquals("2000", m.get("dst_port_nat"));
		assertEquals("3", m.get("packets"));
		assertEquals("4096", m.get("bytes"));
	}

	@Test
	public void csvTest2_6_1() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[fw4_traffic] [...] 2015-01-14 08:56:45,localhost,0,5,0,4,0,350")));
		assertEquals("fw4_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("0", m.get("allow_packets"));
		assertEquals("5", m.get("deny_packets"));
		assertEquals("0", m.get("sessions"));
		assertEquals("4", m.get("max_sessions"));
		assertEquals("0", m.get("allow_bytes"));
		assertEquals("350", m.get("deny_bytes"));
	}

	@Test
	public void csvTest2_6_2() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[fw4_traffic] [...] 2015-01-14 08:56:50,localhost,0,6,0,4,0,685")));
		assertEquals("fw4_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:50", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("0", m.get("allow_packets"));
		assertEquals("6", m.get("deny_packets"));
		assertEquals("0", m.get("sessions"));
		assertEquals("4", m.get("max_sessions"));
		assertEquals("0", m.get("allow_bytes"));
		assertEquals("685", m.get("deny_bytes"));
	}

	@Test
	public void csvTest2_6_3() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw4_traffic] [...] 2013-08-29 07:56:45,mindori_machine,110,115,5,10,1110,350")));
		assertEquals("fw4_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2013-08-29 07:56:45", m.get("time"));
		assertEquals("mindori_machine", m.get("machine_name"));
		assertEquals("110", m.get("allow_packets"));
		assertEquals("115", m.get("deny_packets"));
		assertEquals("5", m.get("sessions"));
		assertEquals("10", m.get("max_sessions"));
		assertEquals("1110", m.get("allow_bytes"));
		assertEquals("350", m.get("deny_bytes"));
	}

	@Test
	public void csvTest2_6_4() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[fw4_traffic] [...] 10:00:00, ,0,5,0,4,0,350")));
		assertEquals("fw4_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("10:00:00", m.get("time"));
		assertEquals(" ", m.get("machine_name"));
		assertEquals("0", m.get("allow_packets"));
		assertEquals("5", m.get("deny_packets"));
		assertEquals("0", m.get("sessions"));
		assertEquals("4", m.get("max_sessions"));
		assertEquals("0", m.get("allow_bytes"));
		assertEquals("350", m.get("deny_bytes"));
	}

	@Test
	public void csvTest2_7_1() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw4_rule_traffic] [...] 2015-01-14 08:56:50,localhost,2,ALLOW,0,0,0,3")));
		assertEquals("fw4_rule_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:50", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("2", m.get("fw_rule_id"));
		assertEquals("ALLOW", m.get("action"));
		assertEquals("0", m.get("packets"));
		assertEquals("0", m.get("bytes"));
		assertEquals("0", m.get("sessions"));
		assertEquals("3", m.get("max_sessions"));
	}

	@Test
	public void csvTest2_7_2() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw4_rule_traffic] [...] 2015-01-14 08:56:50,localhost,0,DENY,6,685,0,0")));
		assertEquals("fw4_rule_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:50", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("DENY", m.get("action"));
		assertEquals("6", m.get("packets"));
		assertEquals("685", m.get("bytes"));
		assertEquals("0", m.get("sessions"));
		assertEquals("0", m.get("max_sessions"));
	}

	@Test
	public void csvTest2_7_3() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw4_rule_traffic] [...] 2015-01-14 08:56:45,localhost,0,DENY,5,350,0,0")));
		assertEquals("fw4_rule_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("DENY", m.get("action"));
		assertEquals("5", m.get("packets"));
		assertEquals("350", m.get("bytes"));
		assertEquals("0", m.get("sessions"));
		assertEquals("0", m.get("max_sessions"));
	}

	@Test
	public void csvTest2_7_4() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw4_rule_traffic] [...] 2015-01-14 08:56:45,localhost,2,ALLOW,0,0,0,3")));
		assertEquals("fw4_rule_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("2", m.get("fw_rule_id"));
		assertEquals("ALLOW", m.get("action"));
		assertEquals("0", m.get("packets"));
		assertEquals("0", m.get("bytes"));
		assertEquals("0", m.get("sessions"));
		assertEquals("3", m.get("max_sessions"));
	}

	@Test
	public void csvTest2_8_1() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[fw6_traffic] [...] 2015-01-14 08:56:45,localhost,0,5,0,4,0,350")));
		assertEquals("fw6_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("0", m.get("allow_packets"));
		assertEquals("5", m.get("deny_packets"));
		assertEquals("0", m.get("sessions"));
		assertEquals("4", m.get("max_sessions"));
		assertEquals("0", m.get("allow_bytes"));
		assertEquals("350", m.get("deny_bytes"));
	}

	@Test
	public void csvTest2_8_2() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[fw6_traffic] [...] 2015-01-14 08:56:50,localhost,0,6,0,4,0,685")));
		assertEquals("fw6_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:50", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("0", m.get("allow_packets"));
		assertEquals("6", m.get("deny_packets"));
		assertEquals("0", m.get("sessions"));
		assertEquals("4", m.get("max_sessions"));
		assertEquals("0", m.get("allow_bytes"));
		assertEquals("685", m.get("deny_bytes"));
	}

	@Test
	public void csvTest2_8_3() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw6_traffic] [...] 2013-08-29 07:56:45,mindori_machine,110,115,5,10,1110,350")));
		assertEquals("fw6_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2013-08-29 07:56:45", m.get("time"));
		assertEquals("mindori_machine", m.get("machine_name"));
		assertEquals("110", m.get("allow_packets"));
		assertEquals("115", m.get("deny_packets"));
		assertEquals("5", m.get("sessions"));
		assertEquals("10", m.get("max_sessions"));
		assertEquals("1110", m.get("allow_bytes"));
		assertEquals("350", m.get("deny_bytes"));
	}

	@Test
	public void csvTest2_8_4() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[fw6_traffic] [...] 10:00:00, ,0,5,0,4,0,350")));
		assertEquals("fw6_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("10:00:00", m.get("time"));
		assertEquals(" ", m.get("machine_name"));
		assertEquals("0", m.get("allow_packets"));
		assertEquals("5", m.get("deny_packets"));
		assertEquals("0", m.get("sessions"));
		assertEquals("4", m.get("max_sessions"));
		assertEquals("0", m.get("allow_bytes"));
		assertEquals("350", m.get("deny_bytes"));
	}

	@Test
	public void csvTest2_9_1() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw6_rule_traffic] [...] 2015-01-14 08:56:50,localhost,2,ALLOW,0,0,0,3")));
		assertEquals("fw6_rule_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:50", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("2", m.get("fw_rule_id"));
		assertEquals("ALLOW", m.get("action"));
		assertEquals("0", m.get("packets"));
		assertEquals("0", m.get("bytes"));
		assertEquals("0", m.get("sessions"));
		assertEquals("3", m.get("max_sessions"));
	}

	@Test
	public void csvTest2_9_2() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw6_rule_traffic] [...] 2015-01-14 08:56:50,localhost,0,DENY,6,685,0,0")));
		assertEquals("fw6_rule_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:50", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("DENY", m.get("action"));
		assertEquals("6", m.get("packets"));
		assertEquals("685", m.get("bytes"));
		assertEquals("0", m.get("sessions"));
		assertEquals("0", m.get("max_sessions"));
	}

	@Test
	public void csvTest2_9_3() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw6_rule_traffic] [...] 2015-01-14 08:56:45,localhost,0,DENY,5,350,0,0")));
		assertEquals("fw6_rule_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("0", m.get("fw_rule_id"));
		assertEquals("DENY", m.get("action"));
		assertEquals("5", m.get("packets"));
		assertEquals("350", m.get("bytes"));
		assertEquals("0", m.get("sessions"));
		assertEquals("0", m.get("max_sessions"));
	}

	@Test
	public void csvTest2_9_4() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[fw6_rule_traffic] [...] 2015-01-14 08:56:45,localhost,2,ALLOW,0,0,0,3")));
		assertEquals("fw6_rule_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("2", m.get("fw_rule_id"));
		assertEquals("ALLOW", m.get("action"));
		assertEquals("0", m.get("packets"));
		assertEquals("0", m.get("bytes"));
		assertEquals("0", m.get("sessions"));
		assertEquals("3", m.get("max_sessions"));
	}

	@Test
	public void csvTest2_10_1() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[nat_traffic] [...] 2013-08-29 07:56:45,mindori_machine,110,350,5,10")));
		assertEquals("nat_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2013-08-29 07:56:45", m.get("time"));
		assertEquals("mindori_machine", m.get("machine_name"));
		assertEquals("110", m.get("packets"));
		assertEquals("350", m.get("bytes"));
		assertEquals("5", m.get("sessions"));
		assertEquals("10", m.get("max_sessions"));
	}

	@Test
	public void csvTest2_10_2() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[nat_traffic] [192.168.0.1] 2013-08-29 07:56:45,mindori_machine,2110,350,5,10")));
		assertEquals("nat_traffic", m.get("log_type"));
		assertEquals("192.168.0.1", m.get("from_ip"));
		assertEquals("2013-08-29 07:56:45", m.get("time"));
		assertEquals("mindori_machine", m.get("machine_name"));
		assertEquals("2110", m.get("packets"));
		assertEquals("350", m.get("bytes"));
		assertEquals("5", m.get("sessions"));
		assertEquals("10", m.get("max_sessions"));
	}

	@Test
	public void csvTest2_10_3() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[nat_traffic] [...] 2014-05-20 07:56:45,localhost,110,350, , ")));
		assertEquals("nat_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2014-05-20 07:56:45", m.get("time"));
		assertEquals("localhost", m.get("machine_name"));
		assertEquals("110", m.get("packets"));
		assertEquals("350", m.get("bytes"));
		assertEquals(" ", m.get("sessions"));
		assertEquals(" ", m.get("max_sessions"));
	}

	@Test
	public void csvTest2_10_4() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[nat_traffic] [...]  , ,110,350,5,10")));
		assertEquals("nat_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals(" ", m.get("time"));
		assertEquals(" ", m.get("machine_name"));
		assertEquals("110", m.get("packets"));
		assertEquals("350", m.get("bytes"));
		assertEquals("5", m.get("sessions"));
		assertEquals("10", m.get("max_sessions"));
	}

	@Test
	public void csvTest2_11_1() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[nat_rule_traffic] [...] 2013-08-29 07:56:45,mindori_machine,110,1,350,5,10")));
		assertEquals("nat_rule_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2013-08-29 07:56:45", m.get("time"));
		assertEquals("mindori_machine", m.get("machine_name"));
		assertEquals("110", m.get("nat_rule_id"));
		assertEquals("1", m.get("packets"));
		assertEquals("350", m.get("bytes"));
		assertEquals("5", m.get("sessions"));
		assertEquals("10", m.get("max_sessions"));
	}

	@Test
	public void csvTest2_11_2() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[nat_rule_traffic] [...] 07:56:45,mindori_machine,110,1,350,5,10")));
		assertEquals("nat_rule_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("07:56:45", m.get("time"));
		assertEquals("mindori_machine", m.get("machine_name"));
		assertEquals("110", m.get("nat_rule_id"));
		assertEquals("1", m.get("packets"));
		assertEquals("350", m.get("bytes"));
		assertEquals("5", m.get("sessions"));
		assertEquals("10", m.get("max_sessions"));
	}

	@Test
	public void csvTest2_11_3() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[nat_rule_traffic] [...] 2013-08-29 07:56:45,mindori_machine, ,1,350,5,10")));
		assertEquals("nat_rule_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2013-08-29 07:56:45", m.get("time"));
		assertEquals("mindori_machine", m.get("machine_name"));
		assertEquals(" ", m.get("nat_rule_id"));
		assertEquals("1", m.get("packets"));
		assertEquals("350", m.get("bytes"));
		assertEquals("5", m.get("sessions"));
		assertEquals("10", m.get("max_sessions"));
	}

	@Test
	public void csvTest2_11_4() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[nat_rule_traffic] [...] 2013-08-29 07:56:45,mindori_machine,110,1,350,5, ")));
		assertEquals("nat_rule_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2013-08-29 07:56:45", m.get("time"));
		assertEquals("mindori_machine", m.get("machine_name"));
		assertEquals("110", m.get("nat_rule_id"));
		assertEquals("1", m.get("packets"));
		assertEquals("350", m.get("bytes"));
		assertEquals("5", m.get("sessions"));
		assertEquals(" ", m.get("max_sessions"));
	}

	@Test
	public void csvTest3_1() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[ips_ddos_detect] [...] 2013-08-29 07:56:45,mindori_machine,DDOS,eediom,0.0.0.0,55,192.168.0.1,80,TCP,-,-,MAC,3,4096,DO,5")));
		assertEquals("ips_ddos_detect", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2013-08-29 07:56:45", m.get("start_time"));
		assertEquals("mindori_machine", m.get("machine_name"));
		assertEquals("DDOS", m.get("attack_name"));
		assertEquals("eediom", m.get("domain_id"));
		assertEquals("0.0.0.0", m.get("src_ip"));
		assertEquals("55", m.get("src_port"));
		assertEquals("192.168.0.1", m.get("dst_ip"));
		assertEquals("80", m.get("dst_port"));
		assertEquals("TCP", m.get("protocol"));
		assertEquals("-", m.get("ip_flag"));
		assertEquals("-", m.get("tcp_flag"));
		assertEquals("MAC", m.get("src_mac"));
		assertEquals("3", m.get("packets"));
		assertEquals("4096", m.get("bytes"));
		assertEquals("DO", m.get("action"));
		assertEquals("5", m.get("dump_id"));
	}

	@Test
	public void csvTest3_2() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[ips_ddos_incident] [...] 2013-08-29 07:56:45,2013-08-29 08:56:45,mindori_machine,DDOS,1,eediom,3,4096,DO")));
		assertEquals("ips_ddos_incident", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2013-08-29 07:56:45", m.get("start_time"));
		assertEquals("2013-08-29 08:56:45", m.get("end_time"));
		assertEquals("mindori_machine", m.get("machine_name"));
		assertEquals("DDOS", m.get("attack_name"));
		assertEquals("1", m.get("priority"));
		assertEquals("eediom", m.get("domain_id"));
		assertEquals("3", m.get("packets"));
		assertEquals("4096", m.get("bytes"));
		assertEquals("DO", m.get("action"));
	}

	@Test
	public void csvTest3_3() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[ips_ddos_traffic] [...] 2015-01-14 08:56:45,mindori,1,2,4,3,5,6,7,8")));
		assertEquals("ips_ddos_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("incoming_pps"));
		assertEquals("2", m.get("incoming_detect_pps"));
		assertEquals("4", m.get("incoming_bps"));
		assertEquals("3", m.get("incoming_detect_bps"));
		assertEquals("5", m.get("outgoing_pps"));
		assertEquals("6", m.get("outgoing_detect_pps"));
		assertEquals("7", m.get("outgoing_bps"));
		assertEquals("8", m.get("outgoing_detect_bps"));
	}

	@Test
	public void csvTest3_4And3_5() {
		// log signature of 3_4, 3_5 are exactly same
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[ips_ddos_domain_traffic] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17")));
		assertEquals("ips_ddos_domain_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("domain_id"));
		assertEquals("2", m.get("incoming_tcp"));
		assertEquals("3", m.get("incoming_detect_tcp"));
		assertEquals("4", m.get("incoming_udp"));
		assertEquals("5", m.get("incoming_detect_udp"));
		assertEquals("6", m.get("incoming_icmp"));
		assertEquals("7", m.get("incoming_detect_icmp"));
		assertEquals("8", m.get("incoming_etc"));
		assertEquals("9", m.get("incoming_detect_etc"));
		assertEquals("10", m.get("outgoing_tcp"));
		assertEquals("11", m.get("outgoing_detect_tcp"));
		assertEquals("12", m.get("outgoing_udp"));
		assertEquals("13", m.get("outgoing_detect_udp"));
		assertEquals("14", m.get("outgoing_icmp"));
		assertEquals("15", m.get("outgoing_detect_icmp"));
		assertEquals("16", m.get("outgoing_etc"));
		assertEquals("17", m.get("outgoing_detect_etc"));

	}

	// @Test
	public void csvTest3_6() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[ips_ddos_domain_traffic] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25")));
		assertEquals("ips_ddos_domain_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("domain_id"));
		assertEquals("2", m.get("incoming_64"));
		assertEquals("3", m.get("incoming_detect_64"));
		assertEquals("4", m.get("incoming_128"));
		assertEquals("5", m.get("incoming_detect_128"));
		assertEquals("6", m.get("incoming_256"));
		assertEquals("7", m.get("incoming_detect_256"));
		assertEquals("8", m.get("incoming_512"));
		assertEquals("9", m.get("incoming_detect_512"));
		assertEquals("10", m.get("incoming_1024"));
		assertEquals("11", m.get("incoming_detect_1024"));
		assertEquals("12", m.get("incoming_1518"));
		assertEquals("13", m.get("incoming_detect_1518"));
		assertEquals("14", m.get("outgoing_64"));
		assertEquals("15", m.get("outgoing_detect_64"));
		assertEquals("16", m.get("outgoing_128"));
		assertEquals("17", m.get("outgoing_detect_128"));
		assertEquals("18", m.get("outgoing_256"));
		assertEquals("19", m.get("outgoing_detect_256"));
		assertEquals("20", m.get("outgoing_512"));
		assertEquals("21", m.get("outgoing_detect_512"));
		assertEquals("22", m.get("outgoing_1024"));
		assertEquals("23", m.get("outgoing_detect_1024"));
		assertEquals("24", m.get("outgoing_1518"));
		assertEquals("25", m.get("outgoing_detect_1518"));
	}

	// @Test
	public void csvTest3_7() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[ips_ddos_domain_traffic] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25")));
		assertEquals("ips_ddos_domain_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("domain_id"));
		assertEquals("2", m.get("incoming_64"));
		assertEquals("3", m.get("incoming_detect_64"));
		assertEquals("4", m.get("incoming_128"));
		assertEquals("5", m.get("incoming_detect_128"));
		assertEquals("6", m.get("incoming_160"));
		assertEquals("7", m.get("incoming_detect_160"));
		assertEquals("8", m.get("incoming_192"));
		assertEquals("9", m.get("incoming_detect_192"));
		assertEquals("10", m.get("incoming_234"));
		assertEquals("11", m.get("incoming_detect_234"));
		assertEquals("12", m.get("incoming_256"));
		assertEquals("13", m.get("incoming_detect_256"));
		assertEquals("14", m.get("outgoing_64"));
		assertEquals("15", m.get("outgoing_detect_64"));
		assertEquals("16", m.get("outgoing_128"));
		assertEquals("17", m.get("outgoing_detect_128"));
		assertEquals("18", m.get("outgoing_160"));
		assertEquals("19", m.get("outgoing_detect_160"));
		assertEquals("20", m.get("outgoing_192"));
		assertEquals("21", m.get("outgoing_detect_192"));
		assertEquals("22", m.get("outgoing_234"));
		assertEquals("23", m.get("outgoing_detect_234"));
		assertEquals("24", m.get("outgoing_256"));
		assertEquals("25", m.get("outgoing_detect_256"));
	}

	@Test
	public void csvTest3_8() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[ips_ddos_domain_traffic] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,2,3,4,5,6,7,8,9,10,11,12,13,14,15")));
		assertEquals("ips_ddos_domain_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("domain_id"));
		assertEquals("2", m.get("incoming_syn"));
		assertEquals("3", m.get("incoming_detect_syn"));
		assertEquals("4", m.get("incoming_fin"));
		assertEquals("5", m.get("incoming_detect_fin"));
		assertEquals("6", m.get("incoming_rst"));
		assertEquals("7", m.get("incoming_detect_rst"));
		assertEquals("8", m.get("incoming_push"));
		assertEquals("9", m.get("incoming_detect_push"));
		assertEquals("10", m.get("incoming_ack"));
		assertEquals("11", m.get("incoming_detect_ack"));
		assertEquals("12", m.get("incoming_urg"));
		assertEquals("13", m.get("incoming_detect_urg"));
		assertEquals("14", m.get("incoming_etc"));
		assertEquals("15", m.get("incoming_detect_etc"));
		assertEquals("2", m.get("outgoing_syn"));
		assertEquals("3", m.get("outgoing_detect_syn"));
		assertEquals("4", m.get("outgoing_fin"));
		assertEquals("5", m.get("outgoing_detect_fin"));
		assertEquals("6", m.get("outgoing_rst"));
		assertEquals("7", m.get("outgoing_detect_rst"));
		assertEquals("8", m.get("outgoing_push"));
		assertEquals("9", m.get("outgoing_detect_push"));
		assertEquals("10", m.get("outgoing_ack"));
		assertEquals("11", m.get("outgoing_detect_ack"));
		assertEquals("12", m.get("outgoing_urg"));
		assertEquals("13", m.get("outgoing_detect_urg"));
		assertEquals("14", m.get("outgoing_etc"));
		assertEquals("15", m.get("outgoing_detect_etc"));
	}

	@Test
	public void csvTest3_9() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[ips_ddos_domain_learning] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6,7,8,9,10,11")));
		assertEquals("ips_ddos_domain_learning", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("domain_id"));
		assertEquals("2", m.get("all_pps"));
		assertEquals("3", m.get("all_bps"));
		assertEquals("4", m.get("tcp_pps"));
		assertEquals("5", m.get("tcp_bps"));
		assertEquals("6", m.get("udp_pps"));
		assertEquals("7", m.get("udp_bps"));
		assertEquals("8", m.get("icmp_pps"));
		assertEquals("9", m.get("icmp_bps"));
		assertEquals("10", m.get("etc_pps"));
		assertEquals("11", m.get("etc_bps"));
	}

	@Test
	public void csvTest4_1() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[vpn_act_ike] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6")));
		assertEquals("vpn_act_ike", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("start_time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("tunnel_name"));
		assertEquals("2", m.get("remote_gateway"));
		assertEquals("3", m.get("key_exchange_step"));
		assertEquals("4", m.get("key_exchange_result"));
		assertEquals("5", m.get("priority"));
		assertEquals("6", m.get("msg"));
	}

	@Test
	public void csvTest4_2() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[vpn_act_ipsec] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6,7")));
		assertEquals("vpn_act_ipsec", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("start_time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("remote_gateway"));
		assertEquals("2", m.get("direction"));
		assertEquals("3", m.get("protocol"));
		assertEquals("4", m.get("priority"));
		assertEquals("5", m.get("spi"));
		assertEquals("6", m.get("action"));
		assertEquals("7", m.get("msg"));
	}

	@Test
	public void csvTest4_3() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[vpn_act_event] [...] 2015-01-14 08:56:45,mindori,message")));
		assertEquals("vpn_act_event", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("start_time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("message", m.get("msg"));
	}

	@Test
	public void csvTest4_4() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[vpn_cnt_line_use] [...] 2015-01-14 08:56:45,mindori,202.1.45.60,2,30,40,1005,4096,80,70,5 sec,OK")));
		assertEquals("vpn_cnt_line_use", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("202.1.45.60", m.get("connection_ip"));
		assertEquals("2", m.get("interface"));
		assertEquals("30", m.get("tx_speed"));
		assertEquals("40", m.get("rx_speed"));
		assertEquals("1005", m.get("tx_bytes"));
		assertEquals("4096", m.get("rx_bytes"));
		assertEquals("80", m.get("tx_usages"));
		assertEquals("70", m.get("rx_usages"));
		assertEquals("5 sec", m.get("fault_duration"));
		assertEquals("OK", m.get("connection_status"));
	}

	@Test
	public void csvTest4_5() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[vpn_cnt_tunnel_use] [...] 2015-01-14 08:56:45,mindori,1,2,3")));
		assertEquals("vpn_cnt_tunnel_use", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("tunnels"));
		assertEquals("2", m.get("normal_tunnels"));
		assertEquals("3", m.get("abnormal_tunnels"));
	}

	@Test
	public void csvTest4_6() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[vpn_cnt_traffic_remotegw] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6")));
		assertEquals("vpn_cnt_traffic_remotegw", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("remote_gateway"));
		assertEquals("2", m.get("encryption_packets"));
		assertEquals("3", m.get("encryption_bytes"));
		assertEquals("4", m.get("decryption_packets"));
		assertEquals("5", m.get("decryption_bytes"));
		assertEquals("6", m.get("usage"));
	}

	// @Test
	public void csvTest4_7() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[vpn_cnt_traffic_remotegw] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6")));
		assertEquals("vpn_cnt_traffic_remotegw", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("tunnel_id"));
		assertEquals("2", m.get("encryption_packets"));
		assertEquals("3", m.get("encryption_bytes"));
		assertEquals("4", m.get("decryption_packets"));
		assertEquals("5", m.get("decryption_bytes"));
		assertEquals("6", m.get("usage"));
	}

	@Test
	public void csvTest4_8() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[vpn_cnt_status_remotegw] [...] 2015-01-14 08:56:45,mindori,1,2,3,4")));
		assertEquals("vpn_cnt_status_remotegw", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("connection_name"));
		assertEquals("2", m.get("remote_gateway"));
		assertEquals("3", m.get("src_interface"));
		assertEquals("4", m.get("round_trip_time"));
	}

	@Test
	public void csvTest4_9() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[vpn_cnt_speed_if] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5")));
		assertEquals("vpn_cnt_speed_if", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("interface"));
		assertEquals("2", m.get("src_ip"));
		assertEquals("3", m.get("dst_ip"));
		assertEquals("4", m.get("tx_speed"));
		assertEquals("5", m.get("rx_speed"));
	}

	@Test
	public void csvTest4_10() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[sslvpn3_act_access] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5")));
		assertEquals("sslvpn3_act_access", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("server_ip"));
		assertEquals("2", m.get("server_port"));
		assertEquals("3", m.get("user_id"));
		assertEquals("4", m.get("user_ip"));
		assertEquals("5", m.get("assign_ip"));
	}

	@Test
	public void csvTest4_11() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[sslvpn3_act_auth] [...] 2015-01-14 08:56:45,mindori,3,4,5,5,5")));
		assertEquals("sslvpn3_act_auth", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("3", m.get("user_id"));
		assertEquals("4", m.get("user_ip"));
		assertEquals("5", m.get("assign_ip"));
		assertEquals("5", m.get("action"));
		assertEquals("5", m.get("desc"));
	}

	@Test
	public void csvTest4_12() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[sslvpn3_act_cert_issue] [...] 2015-01-14 08:56:45,mindori,3,4,2015-01-14 10:56:45,5")));
		assertEquals("sslvpn3_act_cert_issue", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("3", m.get("user_id"));
		assertEquals("4", m.get("user_dn"));
		assertEquals("2015-01-14 10:56:45", m.get("certtime"));
		assertEquals("5", m.get("status"));
	}

	@Test
	public void csvTest4_13() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[sslvpn3_cnt_traffic] [...] 2015-01-14 08:56:45,mindori,3,4,5")));
		assertEquals("sslvpn3_cnt_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("3", m.get("inbound_bytes"));
		assertEquals("4", m.get("outbound_bytes"));
		assertEquals("5", m.get("access_cnt"));
	}

	@Test
	public void csvTest4_14() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[sslvpn3_cnt_server_access] [...] 2015-01-14 08:56:45,mindori,3,5")));
		assertEquals("sslvpn3_cnt_server_access", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("3", m.get("server_ip"));
		assertEquals("5", m.get("access_cnt"));
	}

	@Test
	public void csvTest4_15() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[sslvpn3_cnt_login] [...] 2015-01-14 08:56:45,mindori,3")));
		assertEquals("sslvpn3_cnt_login", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("3", m.get("login_cnt"));
	}

	@Test
	public void csvTest4_16() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[sslvpn3_cnt_tunnel] [...] 2015-01-14 08:56:45,mindori,3,4")));
		assertEquals("sslvpn3_cnt_tunnel", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("3", m.get("web_tunnel_cnt"));
		assertEquals("4", m.get("full_tunnel_cnt"));
	}

	@Test
	public void csvTest4_17() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[sslvpn_act_access] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,http://mindori.com,mindori.com,POST,1.1,GOOD")));
		assertEquals("sslvpn_act_access", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("user_id"));
		assertEquals("2", m.get("src_ip"));
		assertEquals("3", m.get("src_port"));
		assertEquals("4", m.get("dst_ip"));
		assertEquals("5", m.get("dst_port"));
		assertEquals("http://mindori.com", m.get("access_url"));
		assertEquals("mindori.com", m.get("request_uri"));
		assertEquals("POST", m.get("http_command"));
		assertEquals("1.1", m.get("http_version"));
		assertEquals("GOOD", m.get("result"));

	}

	@Test
	public void csvTest4_18() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[sslvpn_act_session] [...] 2015-01-14 08:56:45,2015-01-14 10:56:45,mindori,1,2,3,TCP,1000,600,-")));
		assertEquals("sslvpn_act_session", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("start_time"));
		assertEquals("2015-01-14 10:56:45", m.get("end_time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("user_id"));
		assertEquals("2", m.get("src_ip"));
		assertEquals("3", m.get("src_port"));
		assertEquals("TCP", m.get("protocol"));
		assertEquals("1000", m.get("bytes_forward"));
		assertEquals("600", m.get("bytes_backward"));
		assertEquals("-", m.get("terminate_reason"));
	}

	@Test
	public void csvTest4_19() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[sslvpn_act_auth] [...] 2015-01-14 08:56:45,mindori,mindori_session,1,2,-")));
		assertEquals("sslvpn_act_auth", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("mindori_session", m.get("session_id"));
		assertEquals("1", m.get("user_id"));
		assertEquals("2", m.get("src_ip"));
		assertEquals("-", m.get("result"));
	}

	@Test
	public void csvTest4_20() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[sslvpn_cnt_traffic] [...] 2015-01-14 08:56:45,mindori,0,1,2")));
		assertEquals("sslvpn_cnt_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("0", m.get("transactions"));
		assertEquals("1", m.get("incoming_bytes"));
		assertEquals("2", m.get("outgoing_bytes"));
	}

	@Test
	public void csvTest4_21() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[sslvpn_cnt_service_traffic] [...] 2015-01-14 08:56:45,mindori,srv,0,1,2")));
		assertEquals("sslvpn_cnt_service_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("srv", m.get("service_name"));
		assertEquals("0", m.get("transactions"));
		assertEquals("1", m.get("incoming_bytes"));
		assertEquals("2", m.get("outgoing_bytes"));
	}

	@Test
	public void csvTest4_22() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[sslvpn_cnt_user_traffic] [...] 2015-01-14 08:56:45,mindori,srv,0,1,2")));
		assertEquals("sslvpn_cnt_user_traffic", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("srv", m.get("user_id"));
		assertEquals("0", m.get("transactions"));
		assertEquals("1", m.get("incoming_bytes"));
		assertEquals("2", m.get("outgoing_bytes"));
	}

	@Test
	public void csvTest4_23() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[sslvpn_cnt_concurrent] [...] 2015-01-14 08:56:45,mindori,1,10")));
		assertEquals("sslvpn_cnt_concurrent", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("concurrents"));
		assertEquals("10", m.get("request_counts"));
	}

	@Test
	public void csvTest5_1() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[app_act_antispam] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6,7,8,9,10,11,12")));
		assertEquals("app_act_antispam", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("start_time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("fw_rule_id"));
		assertEquals("2", m.get("src_ip"));
		assertEquals("3", m.get("dst_ip"));
		assertEquals("4", m.get("protocol"));
		assertEquals("5", m.get("direction"));
		assertEquals("6", m.get("sender_addr"));
		assertEquals("7", m.get("receiver_addr"));
		assertEquals("8", m.get("mail_bytes"));
		assertEquals("9", m.get("mail_status"));
		assertEquals("10", m.get("spam_type"));
		assertEquals("11", m.get("action"));
		assertEquals("12", m.get("msg"));
	}

	@Test
	public void csvTest5_2() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[app_act_ftp] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,PUT,12")));
		assertEquals("app_act_ftp", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("start_time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("fw_rule_id"));
		assertEquals("2", m.get("src_ip"));
		assertEquals("3", m.get("dst_ip"));
		assertEquals("4", m.get("inspect_type"));
		assertEquals("5", m.get("action"));
		assertEquals("PUT", m.get("ftp_command"));
		assertEquals("12", m.get("msg"));
	}

	@Test
	public void csvTest5_3() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[app_act_antivirus] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6,7,8,9,10")));
		assertEquals("app_act_antivirus", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("start_time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("fw_rule_id"));
		assertEquals("2", m.get("src_ip"));
		assertEquals("3", m.get("dst_ip"));
		assertEquals("4", m.get("file_size"));
		assertEquals("5", m.get("file_name"));
		assertEquals("6", m.get("virus_name"));
		assertEquals("7", m.get("protocol"));
		assertEquals("8", m.get("direction"));
		assertEquals("9", m.get("action"));
		assertEquals("10", m.get("msg"));
	}

	@Test
	public void csvTest5_4() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[app_act_webclient_all] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6,7,9,10")));
		assertEquals("app_act_webclient_all", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("start_time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("fw_rule_id"));
		assertEquals("2", m.get("client_ip"));
		assertEquals("3", m.get("server_ip"));
		assertEquals("4", m.get("tx_bytes"));
		assertEquals("5", m.get("rx_bytes"));
		assertEquals("6", m.get("http_command"));
		assertEquals("7", m.get("transaction_id"));
		assertEquals("9", m.get("result"));
		assertEquals("10", m.get("msg"));
	}

	@Test
	public void csvTest5_5() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[app_act_webclient_limit] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6,7,9,10")));
		assertEquals("app_act_webclient_limit", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("start_time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("fw_rule_id"));
		assertEquals("2", m.get("client_ip"));
		assertEquals("3", m.get("server_ip"));
		assertEquals("4", m.get("tx_bytes"));
		assertEquals("5", m.get("rx_bytes"));
		assertEquals("6", m.get("http_command"));
		assertEquals("7", m.get("transaction_id"));
		assertEquals("9", m.get("result"));
		assertEquals("10", m.get("msg"));
	}

	@Test
	public void csvTest5_6() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[app_act_urlblock] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6")));
		assertEquals("app_act_urlblock", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("start_time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("fw_rule_id"));
		assertEquals("2", m.get("src_ip"));
		assertEquals("3", m.get("dst_ip"));
		assertEquals("4", m.get("category"));
		assertEquals("5", m.get("dst_name"));
		assertEquals("6", m.get("uri"));
	}

	@Test
	public void csvTest5_7() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[app_cnt_antispam] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6,7")));
		assertEquals("app_cnt_antispam", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("start_time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("all_counts"));
		assertEquals("2", m.get("allow_counts"));
		assertEquals("3", m.get("detect_counts"));
		assertEquals("4", m.get("alarm_counts"));
		assertEquals("5", m.get("block_counts"));
		assertEquals("6", m.get("protocol"));
		assertEquals("7", m.get("direction"));
	}

	@Test
	public void csvTest5_8() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[app_cnt_ftp] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5")));
		assertEquals("app_cnt_ftp", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("start_time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("all_counts"));
		assertEquals("2", m.get("upload_counts"));
		assertEquals("3", m.get("download_counts"));
		assertEquals("4", m.get("etc_counts"));
		assertEquals("5", m.get("detect_counts"));
	}

	@Test
	public void csvTest5_9() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[app_cnt_antivirus] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5")));
		assertEquals("app_cnt_antivirus", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("start_time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("all_counts"));
		assertEquals("2", m.get("block_counts"));
		assertEquals("3", m.get("detect_counts"));
		assertEquals("4", m.get("protocol"));
		assertEquals("5", m.get("direction"));
	}

	@Test
	public void csvTest5_10() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[app_cnt_webclient_all] [...] 2015-01-14 08:56:45,mindori,1,2")));
		assertEquals("app_cnt_webclient_all", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("start_time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("tx_bytes"));
		assertEquals("2", m.get("rx_bytes"));
	}

	@Test
	public void csvTest5_11() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[app_cnt_webclient_limit] [...] 2015-01-14 08:56:45,mindori,1,2")));
		assertEquals("app_cnt_webclient_limit", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("start_time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("content_limit_counts"));
		assertEquals("2", m.get("abnormal_limit_counts"));
	}

	@Test
	public void csvTest5_12() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[app_cnt_urlblock] [...] 2015-01-14 08:56:45,mindori,1")));
		assertEquals("app_cnt_urlblock", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("start_time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("block_counts"));
	}

	@Test
	public void csvTest5_13() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[app_act_control_detect] [...] 2015-01-14 08:56:45,mindori,192.168.0.1,80,210.50.7.102,55,1,2,3,4,5,6,7,8")));
		assertEquals("app_act_control_detect", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("192.168.0.1", m.get("src_ip"));
		assertEquals("80", m.get("src_port"));
		assertEquals("210.50.7.102", m.get("dst_ip"));
		assertEquals("55", m.get("dst_port"));
		assertEquals("1", m.get("fw_rule_id"));
		assertEquals("2", m.get("profile_id"));
		assertEquals("3", m.get("category_id"));
		assertEquals("4", m.get("application_id"));
		assertEquals("5", m.get("function_id"));
		assertEquals("6", m.get("packet_dump_id"));
		assertEquals("7", m.get("action"));
		assertEquals("8", m.get("msg"));
	}

	@Test
	public void csvTest5_14() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[app_cnt_control_papplication] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17")));
		assertEquals("app_cnt_control_papplication", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("application_id"));
		assertEquals("2", m.get("detect_counts"));
		assertEquals("3", m.get("block_counts"));
		assertEquals("4", m.get("access_detect_counts"));
		assertEquals("5", m.get("access_block_counts"));
		assertEquals("6", m.get("login_detect_counts"));
		assertEquals("7", m.get("login_block_counts"));
		assertEquals("8", m.get("message_detect_counts"));
		assertEquals("9", m.get("message_block_counts"));
		assertEquals("10", m.get("file_detect_counts"));
		assertEquals("11", m.get("file_block_counts"));
		assertEquals("12", m.get("file_size_detect_counts"));
		assertEquals("13", m.get("file_size_block_counts"));
		assertEquals("14", m.get("audio_detect_counts"));
		assertEquals("15", m.get("audio_block_counts"));
		assertEquals("16", m.get("video_detect_counts"));
		assertEquals("17", m.get("video_block_counts"));
	}

	@Test
	public void csvTest5_15() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[app_cnt_control_pcategory] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17")));
		assertEquals("app_cnt_control_pcategory", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("category_id"));
		assertEquals("2", m.get("detect_counts"));
		assertEquals("3", m.get("block_counts"));
		assertEquals("4", m.get("access_detect_counts"));
		assertEquals("5", m.get("access_block_counts"));
		assertEquals("6", m.get("login_detect_counts"));
		assertEquals("7", m.get("login_block_counts"));
		assertEquals("8", m.get("message_detect_counts"));
		assertEquals("9", m.get("message_block_counts"));
		assertEquals("10", m.get("file_detect_counts"));
		assertEquals("11", m.get("file_block_counts"));
		assertEquals("12", m.get("file_size_detect_counts"));
		assertEquals("13", m.get("file_size_block_counts"));
		assertEquals("14", m.get("audio_detect_counts"));
		assertEquals("15", m.get("audio_block_counts"));
		assertEquals("16", m.get("video_detect_counts"));
		assertEquals("17", m.get("video_block_counts"));
	}

	@Test
	public void csvTest5_16() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[app_cnt_control_pprofile] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17")));
		assertEquals("app_cnt_control_pprofile", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("profile_id"));
		assertEquals("2", m.get("detect_counts"));
		assertEquals("3", m.get("block_counts"));
		assertEquals("4", m.get("access_detect_counts"));
		assertEquals("5", m.get("access_block_counts"));
		assertEquals("6", m.get("login_detect_counts"));
		assertEquals("7", m.get("login_block_counts"));
		assertEquals("8", m.get("message_detect_counts"));
		assertEquals("9", m.get("message_block_counts"));
		assertEquals("10", m.get("file_detect_counts"));
		assertEquals("11", m.get("file_block_counts"));
		assertEquals("12", m.get("file_size_detect_counts"));
		assertEquals("13", m.get("file_size_block_counts"));
		assertEquals("14", m.get("audio_detect_counts"));
		assertEquals("15", m.get("audio_block_counts"));
		assertEquals("16", m.get("video_detect_counts"));
		assertEquals("17", m.get("video_block_counts"));
	}

	@Test
	public void csvTest5_17() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[app_act_webserver_protect] [...] 2015-01-14 08:56:45,mindori,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15")));
		assertEquals("app_act_webserver_protect", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("fw_rule_id"));
		assertEquals("2", m.get("profile_id"));
		assertEquals("3", m.get("attack_id"));
		assertEquals("4", m.get("attack_description"));
		assertEquals("5", m.get("src_ip"));
		assertEquals("6", m.get("src_port"));
		assertEquals("7", m.get("dst_ip"));
		assertEquals("8", m.get("dst_port"));
		assertEquals("9", m.get("dst_url"));
		assertEquals("10", m.get("packets"));
		assertEquals("11", m.get("bytes"));
		assertEquals("12", m.get("priority"));
		assertEquals("13", m.get("action"));
		assertEquals("14", m.get("mails"));
		assertEquals("15", m.get("dump_id"));
	}

	@Test
	public void csvTest5_18() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[app_cnt_webserver_protect] [...] 2015-01-14 08:56:45,mindori,1,2,3,4")));
		assertEquals("app_cnt_webserver_protect", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("all_counts"));
		assertEquals("2", m.get("all_bytes"));
		assertEquals("3", m.get("detect_counts"));
		assertEquals("4", m.get("detect_bytes"));
	}

	@Test
	public void csvTest5_19() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p
				.parse(line(convertTsv("[app_act_officekeeper_list] [...] 2015-01-14 08:56:45,mindori,1,2,3,4")));
		assertEquals("app_act_officekeeper_list", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("action"));
		assertEquals("2", m.get("block_ip"));
		assertEquals("3", m.get("server"));
		assertEquals("4", m.get("redirect_url"));
	}

	@Test
	public void csvTest5_20() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("[app_act_officekeeper_list] [...] 2015-01-14 08:56:45,mindori,1,2,3")));
		assertEquals("app_act_officekeeper_list", m.get("log_type"));
		assertEquals("...", m.get("from_ip"));
		assertEquals("2015-01-14 08:56:45", m.get("time"));
		assertEquals("mindori", m.get("machine_name"));
		assertEquals("1", m.get("src_ip"));
		assertEquals("2", m.get("action"));
		assertEquals("3", m.get("redirect_url"));
	}

	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}

	private String convertTsv(String line) {
		return line.replaceAll(",", "\t");
	}
}
