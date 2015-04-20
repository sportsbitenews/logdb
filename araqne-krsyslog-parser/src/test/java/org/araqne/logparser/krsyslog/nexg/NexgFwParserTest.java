package org.araqne.logparser.krsyslog.nexg;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.araqne.logparser.krsyslog.nexg.NexgFwParser;
import org.junit.Test;

public class NexgFwParserTest {
	@Test
	public void testSample() {
		String line = "2015-04-14 14:00:05 INET-NOTICE: NAME=모두허용 PROTO=TCP IN=eth1 SRC=200.200.200.1 SPT=37328 OUT=eth0 DST=125.209.222.142 DPT=80 ACT=OPEN USER=NONE APP=naver-service/host-access NAT_NAME=200.200.200.0/24_외부 SNAT_SRC=10.200.6.29 SNAT_SPT=10328 REASON='Application Detect' START=05/21/2014-17:20:22 PACKETS=4 BYTES=776";
		
		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		NexgFwParser p = new NexgFwParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("2015-04-14 14:00:05", m.get("DATETIME"));
		assertEquals("INET-NOTICE", m.get("LOG_CATEGORY"));
		assertEquals("모두허용", m.get("NAME"));
		assertEquals("TCP", m.get("PROTO"));
		assertEquals("eth1", m.get("IN"));
		assertEquals("200.200.200.1", m.get("SRC"));
		assertEquals("37328", m.get("SPT"));
		assertEquals("eth0", m.get("OUT"));
		assertEquals("125.209.222.142", m.get("DST"));
		assertEquals("80", m.get("DPT"));
		assertEquals("OPEN", m.get("ACT"));
		assertEquals("NONE", m.get("USER"));
		assertEquals("naver-service/host-access", m.get("APP"));
		assertEquals("200.200.200.0/24_외부", m.get("NAT_NAME"));
		assertEquals("10.200.6.29", m.get("SNAT_SRC"));
		assertEquals("10328", m.get("SNAT_SPT"));
		assertEquals("Application Detect", m.get("REASON"));
		assertEquals("05/21/2014-17:20:22", m.get("START"));
		assertEquals("4", m.get("PACKETS"));
		assertEquals("776", m.get("BYTES"));
	}
	
	@Test
	public void testSample2() {
		String line = "2015-04-14 14:00:05 INET-NOTICE: NAME=Default PROTO=UDP IN=eth0.7 SRC=0.0.0.0 SPT=68 OUT=lo DST=255.255.255.255 DPT=67 ACT=Deny USER=NONE APP=Exception MISC='PACKETS=\"1\" BYTES=\"324\"'";
		
		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		NexgFwParser p = new NexgFwParser();
		Map<String, Object> m = p.parse(log);
		
		assertEquals("2015-04-14 14:00:05", m.get("DATETIME"));
		assertEquals("INET-NOTICE", m.get("LOG_CATEGORY"));
		assertEquals("Default", m.get("NAME"));
		assertEquals("1", m.get("PACKETS"));
		assertEquals("324", m.get("BYTES"));
	}
	
	@Test
	public void testSample3() {
		String line = "2015-04-14 13:59:33 INET-NOTICE: NAME=Default PROTO=IGMP IN=eth0.7 SRC=10.10.189.254 SPT=NONE OUT=lo DST=224.0.0.1 DPT=NONE ACT=Deny USER=NONE APP=Exception MISC='REASON=\"Denial Session\" START=\"04/14/2015-13:59:22\" END=\"04/14/2015-13:59:32\" PACKETS=\"0\" BYTES=\"0\"'";
		
		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		NexgFwParser p = new NexgFwParser();
		Map<String, Object> m = p.parse(log);
		
		assertEquals("2015-04-14 13:59:33", m.get("DATETIME"));
		assertEquals("INET-NOTICE", m.get("LOG_CATEGORY"));
		assertEquals("NONE", m.get("USER"));
		assertEquals("Denial Session", m.get("REASON"));
		assertEquals("04/14/2015-13:59:22", m.get("START"));
		assertEquals("04/14/2015-13:59:32", m.get("END"));
		assertEquals("0", m.get("PACKETS"));
		assertEquals("0", m.get("BYTES"));
	}
	
	@Test
	public void testSample4() {
		String line = "2015-04-14 13:59:33 INET-NOTICE: NAME=hub_and_spoke1(CHILD_SA) SRC=10.101.0.48 SPT=500 DST=10.101.0.208 DPT=500 ACT=Established USER=None MISC='REASON=\"CHILD_SA established. SPI(c55e1de0_i cb197da9_o), TS(10.10.8.0/24 === 10.10.0.0/16 ), REQID(1), , IKE_SA(hub_and_spoke1:1)\" LOCALID=\"10.101.0.48\" PEERID=\"10.101.0.208\" AUTH_TYPE=\"pre-shared key\" ENC=\"ARIA_CBC_128\" HASH=\"HMAC_SHA2_256_128\"  LIFETIME=\"21s\" FLAGS=\"Initiator/tunnel\"'";
		
		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		NexgFwParser p = new NexgFwParser();
		Map<String, Object> m = p.parse(log);
		
		assertEquals("2015-04-14 13:59:33", m.get("DATETIME"));
		assertEquals("INET-NOTICE", m.get("LOG_CATEGORY"));
		assertEquals("CHILD_SA established. SPI(c55e1de0_i cb197da9_o), TS(10.10.8.0/24 === 10.10.0.0/16 ), REQID(1), , IKE_SA(hub_and_spoke1:1)", m.get("REASON"));
		assertEquals("Initiator/tunnel", m.get("FLAGS"));
	}
	
	@Test
	public void testSample5() {
		String line = "2015-04-14 13:59:33 INET-NOTICE: NAME=hub_and_spoke1(CHILD_SA) SRC=10.101.0.48 SPT=500 DST=10.101.0.208 DPT=500 ACT=Established MISC='REASON=\"CHILD_SA established. SPI(c55e1de0_i cb197da9_o), TS(10.10.8.0/24 === 10.10.0.0/16 ), REQID(1), , IKE_SA(hub_and_spoke1:1)\" LOCALID=\"10.101.0.48\" PEERID=\"10.101.0.208\" AUTH_TYPE=\"pre-shared key\" ENC=\"ARIA_CBC_128\" HASH=\"HMAC_SHA2_256_128\"  LIFETIME=\"21s\" FLAGS=\"Initiator/tunnel\"' USER=NONE";
		
		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		NexgFwParser p = new NexgFwParser();
		Map<String, Object> m = p.parse(log);
		
		assertEquals("2015-04-14 13:59:33", m.get("DATETIME"));
		assertEquals("CHILD_SA established. SPI(c55e1de0_i cb197da9_o), TS(10.10.8.0/24 === 10.10.0.0/16 ), REQID(1), , IKE_SA(hub_and_spoke1:1)", m.get("REASON"));
		assertEquals("Initiator/tunnel", m.get("FLAGS"));
		assertEquals("NONE", m.get("USER"));
	}
}
