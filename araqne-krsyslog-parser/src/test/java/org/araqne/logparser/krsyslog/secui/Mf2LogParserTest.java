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
import org.junit.Test;

/**
 * @since 1.9.2
 * @author xeraph
 * 
 */
public class Mf2LogParserTest {
	@Test
	public void csvTest2_1_1() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p
				.parse(line("[fw4_allow] [...] 2015-01-14 09:21:41,2015-01-14 09:21:46,5,KNIAFWIN1,93,Undefined,42.1.52.79,53363,172.18.1.20,33008,TCP,EXT,4,5,366,704, ,3Way OK / FIN2 [SAF:SAF],-"));
		assertEquals("fw4_allow", m.get("type"));
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
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p
				.parse(line("[fw4_allow] [...] 2015-01-14 09:21:43,2015-01-14 09:21:50,7,KNIAFWIN1,93,Undefined,42.1.52.81,50346,172.18.1.20,38108,TCP,EXT,6,7,494,3729, ,3Way OK / FIN2 [SAF:SAF],-"));
		assertEquals("fw4_allow", m.get("type"));
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
	
	// TODO csvTest2_1_3, csvTest2_1_4
	@Test
	public void csvTest2_1_3() {
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p
				.parse(line("[fw4_allow] [...] 2015-01-14 09:21:14,2015-01-14 09:21:45,31,KNIAFWIN1,Local Out,Undefined,172.100.11.4,-,172.16.33.5,3,ICMP,LOCAL,6,0,580,0, ,-,-"));
		assertEquals("fw4_allow", m.get("type"));
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
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p
				.parse(line("[fw4_allow] [...] 2015-01-14 09:21:37,2015-01-14 09:21:42,5,KNIAFWIN1,93,Undefined,42.1.52.81,50282,172.18.1.20,38108,TCP,EXT,6,7,494,3729, ,3Way OK / FIN2 [SAF:SAF],-"));
		assertEquals("fw4_allow", m.get("type"));
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
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p
				.parse(line("[fw4_deny] [...] 2015-01-14 09:21:52,2015-01-14 09:21:52,0,KNIAFWIN1,0,Undefined,0.0.0.0,68,255.255.255.255,67,UDP,EXT,1,335, ,-,Deny by Deny Rule"));
		assertEquals("fw4_deny", m.get("type"));
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
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p
				.parse(line("[fw4_deny] [...] 2015-01-14 09:24:02,2015-01-14 09:24:02,0,master.fw.com,1,Undefined,0.0.0.0,68,255.255.255.255,67,UDP,EXT,1,335, ,-,Deny by Deny Rule"));
		assertEquals("fw4_deny", m.get("type"));
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
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p
				.parse(line("[fw4_deny] [...] 2015-01-14 08:56:46,2015-01-14 08:56:46,0,localhost,0,Undefined,172.100.40.2,-,224.0.0.18,-,VRRP,EXT,1,70, ,-,Deny by Deny Rule"));
		assertEquals("fw4_deny", m.get("type"));
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
		Mf2LogParser p = new Mf2LogParser();
		Map<String, Object> m = p
				.parse(line("[fw4_deny] [...] 2015-01-14 09:21:50,2015-01-14 09:21:50,0,KNIAFWIN1,0,Undefined,172.31.0.9,137,172.31.255.255,137,UDP,EXT,1,96, ,-,Deny by Deny Rule"));
		assertEquals("fw4_deny", m.get("type"));
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
	
	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
