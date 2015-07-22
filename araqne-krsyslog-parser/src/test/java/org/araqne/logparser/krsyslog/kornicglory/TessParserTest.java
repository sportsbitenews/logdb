package org.araqne.logparser.krsyslog.kornicglory;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class TessParserTest {
	@Test
	public void test() {
		String line = "EventName=\"malware-virut-irc.11071201@\" SigIndex=1008971 Severity=Low Time=\"2013/02/26 10:11:57\" Protocol=TCP AttackerIP=148.1.191.238 AttackerPort=1400 VictimIP=69.43.161.177 VictimPort=80 Count=1 PktCount=1 Pattern=\"NICK VirUs-\" Direct=\"Outbound\" SensorIP=\"192.168.70.81\" Packet=\"00 1C 7F 3F 23\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		TessParser p = new TessParser();
		Map<String, Object> m = p.parse(log);
		assertEquals("malware-virut-irc.11071201@", m.get("eventname"));
		assertEquals("1008971", m.get("sigindex"));
		assertEquals("2013/02/26 10:11:57", m.get("time"));
		assertEquals("00 1C 7F 3F 23", m.get("packet"));
	}

	@Test
	public void test2() {
		String line = "Health info ManagerName=\"\" ManagerIp=\"10.25.6.217\" Time=\"2013/03/21 10:16:00\" CPU_Speed=\"2.1 GHz\" CPU_Num=8 CPU_Usage=\"4 %\" MEMORY_Usage=\"47 %\" HDD_Usage=\"8 %\" PROCESS_Cnt=78 EventLogSaveCnt=880 TrafficLogSaveCnt=9459 TESS_GENERAL_Total=\"1.00 G\" TESS_GENERAL_Used=\"90.25 M\" TESS_GENERAL_Usage=\"8 %\" TESS_INDEX_Total=\"235.00 G\" TESS_INDEXL_Used=\"100.27 G\" TESS_INDEX_Usage=\"42 %\" TESS_LOG_Total=\"135.00 G\" TESS_LOG_Used=\"7.97 G\" TESS_LOG_Usage=\"5 %\" TESS_TRAFFIC_Total=\"330.00 G\" TESS_TRAFFIC_Used=\"136.76 G\" TESS_TRAFFIC_Usage=\"41 %\" TESS_SESSION_Total=\"55.00 G\" TESS_SESSION_Used=\"2.87 G\" TESS_SESSION_Usage=\"5 %\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		TessParser p = new TessParser();
		Map<String, Object> m = p.parse(log);
		assertEquals("", m.get("managername"));
		assertEquals("10.25.6.217", m.get("managerip"));
		assertEquals("2.1 GHz", m.get("cpu_speed"));
		assertEquals("4 %", m.get("cpu_usage"));
		assertEquals("5 %", m.get("tess_session_usage"));
	}

	@Test
	public void test3() {
		String line = "Health info SensorName=\"\uD604\uB300\uC911\uACF5\uC5C5\" SensorIp=\"10.25.6.216\" Connection=1 Tie=\"2013/03/26 18:16:50\" CPU_Usage=\"8 %\" MEMORY_Usage=\"13 %\" HDD_Usage=\"1 %\" PROCESS_Cnt=156 EventPerSecond=\"0.00 \" SessionPerSecond=\"248.46 K\" PacketLossRate=\"0.00 %\" TotalTraffic=\"2.54 G\" MaliciousTraffic=\"19.64 M (0.77 %)\" TotalTrafficPps=\"502.55 K\" MaliciousTrafficPps=\"377.60 K (9.39 %)\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		TessParser p = new TessParser();
		Map<String, Object> m = p.parse(log);
		assertEquals("\uD604\uB300\uC911\uACF5\uC5C5", m.get("sensorname"));
		assertEquals("10.25.6.216", m.get("sensorip"));
		assertEquals("502.55 K", m.get("totaltrafficpps"));
		assertEquals("377.60 K (9.39 %)", m.get("malicioustrafficpps"));
	}

	@Test
	public void test4() {
		String line = "EventName=\"icmp flooding@\" SigIndex=583 Severity=Middle Time=\"2015/07/13 17:45:33\" Protocol=ICMP AttackerIP=198.20.99.130 AttackerPort=0 VictimIP=118.47.74.44 VictimPort=0 Count=1 PktCount=200 Pattern=\"\" Direct=Inbound SensorIP=10.150.230.210 Sensor=\"현대미포조선\" Network=\"NODATA\" VSensor=\"현대미포조선\" Packet=\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		TessParser p = new TessParser();
		Map<String, Object> m = p.parse(log);
		assertEquals("\"", m.get("packet"));
	}
}
