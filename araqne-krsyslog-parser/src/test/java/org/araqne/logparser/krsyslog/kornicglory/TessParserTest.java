package org.araqne.logparser.krsyslog.kornicglory;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class TessParserTest {
	@Test
	public void test() {
		String line = "EventName=\"event_1\" SigIndex=1008971 Severity=Low Time=\"2013/02/26 10:11:57\" Protocol=TCP AttackerIP=1.2.3.4 AttackerPort=1400 VictimIP=5.6.7.8 VictimPort=80 Count=1 PktCount=1 Pattern=\"hoho\" Direct=\"Outbound\" SensorIP=\"3.4.5.6\" Packet=\"00 01 02 03 04\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		TessParser p = new TessParser();
		Map<String, Object> m = p.parse(log);
		assertEquals("event_1", m.get("event_name"));
		assertEquals("1008971", m.get("sig_index"));
		assertEquals("2013/02/26 10:11:57", m.get("time"));
		assertEquals("00 01 02 03 04", m.get("packet"));
	}

	@Test
	public void test2() {
		String line = "Health info ManagerName=\"\" ManagerIp=\"1.2.3.4\" Time=\"2013/03/21 10:16:00\" CPU_Speed=\"2.1 GHz\" CPU_Num=8 CPU_Usage=\"4 %\" MEMORY_Usage=\"47 %\" HDD_Usage=\"8 %\" PROCESS_Cnt=78 EventLogSaveCnt=880 TrafficLogSaveCnt=9459 TESS_GENERAL_Total=\"1.00 G\" TESS_GENERAL_Used=\"90.25 M\" TESS_GENERAL_Usage=\"8 %\" TESS_INDEX_Total=\"235.00 G\" TESS_INDEXL_Used=\"100.27 G\" TESS_INDEX_Usage=\"42 %\" TESS_LOG_Total=\"135.00 G\" TESS_LOG_Used=\"7.97 G\" TESS_LOG_Usage=\"5 %\" TESS_TRAFFIC_Total=\"330.00 G\" TESS_TRAFFIC_Used=\"136.76 G\" TESS_TRAFFIC_Usage=\"41 %\" TESS_SESSION_Total=\"55.00 G\" TESS_SESSION_Used=\"2.87 G\" TESS_SESSION_Usage=\"5 %\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		TessParser p = new TessParser();
		Map<String, Object> m = p.parse(log);
		assertEquals("", m.get("manager_name"));
		assertEquals("1.2.3.4", m.get("manager_ip"));
		assertEquals("2.1 GHz", m.get("cpu_speed"));
		assertEquals("4 %", m.get("cpu_usage"));
		assertEquals("5 %", m.get("tess_session_usage"));
	}

	@Test
	public void test3() {
		String line = "Health info SensorName=\"센서\" SensorIp=\"1.2.3.4\" Connection=1 Tie=\"2013/03/26 18:16:50\" CPU_Usage=\"8 %\" MEMORY_Usage=\"13 %\" HDD_Usage=\"1 %\" PROCESS_Cnt=156 EventPerSecond=\"0.00 \" SessionPerSecond=\"248.46 K\" PacketLossRate=\"0.00 %\" TotalTraffic=\"2.54 G\" MaliciousTraffic=\"19.64 M (0.77 %)\" TotalTrafficPps=\"502.55 K\" MaliciousTrafficPps=\"377.60 K (9.39 %)\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		TessParser p = new TessParser();
		Map<String, Object> m = p.parse(log);
		assertEquals("센서", m.get("sensor_name"));
		assertEquals("1.2.3.4", m.get("sensor_ip"));
		assertEquals("502.55 K", m.get("total_pps"));
		assertEquals("377.60 K (9.39 %)", m.get("mal_pps"));
	}

	@Test
	public void test4() {
		String line = "EventName=\"event@\" SigIndex=583 Severity=Middle Time=\"2015/07/13 17:45:33\" Protocol=ICMP AttackerIP=1.2.3.4 AttackerPort=0 VictimIP=5.6.7.8 VictimPort=0 Count=1 PktCount=200 Pattern=\"\" Direct=Inbound SensorIP=1.3.5.7 Sensor=\"센서\" Network=\"NODATA\" VSensor=\"센서\" Packet=\"";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		TessParser p = new TessParser();
		Map<String, Object> m = p.parse(log);
		assertEquals("\"", m.get("packet"));
	}
}
