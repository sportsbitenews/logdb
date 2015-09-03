package org.araqne.logparser.krsyslog.kornicglory;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

public class TessParser extends V1LogParser {
	private static Map<String, String> COLUMNS = new HashMap<String, String>();
	static {
		COLUMNS.put("EventName", "event_name");
		COLUMNS.put("SigIndex", "sig_index");
		COLUMNS.put("AttackerIP", "attacker_ip");
		COLUMNS.put("AttackerPort", "attacker_port");
		COLUMNS.put("VictimIP", "victim_ip");
		COLUMNS.put("VictimPort", "victim_port");
		COLUMNS.put("PktCount", "pkt_count");
		COLUMNS.put("SensorIp", "sensor_ip");
		COLUMNS.put("ManagerName", "manager_name");
		COLUMNS.put("ManagerIp", "manager_ip");
		COLUMNS.put("EventLogSaveCnt", "event_log_save_cnt");
		COLUMNS.put("TrafficLogSaveCnt", "traffic_log_save_cnt");
		COLUMNS.put("SensorName", "sensor_name");
		COLUMNS.put("EventPerSecond", "event_per_second");
		COLUMNS.put("SessionPerSecond", "session_per_second");
		COLUMNS.put("PacketLossRate", "packet_loss_rate");
		COLUMNS.put("TotalTraffic", "total_traffic");
		COLUMNS.put("MaliciousTraffic", "malicious_traffic");
		COLUMNS.put("TotalTrafficPps", "total_pps");
		COLUMNS.put("MaliciousTrafficPps", "mal_pps");
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> log) {
		String line = (String) log.get("line");
		if (line == null)
			return log;

		try {
			Map<String, Object> m = new HashMap<String, Object>();
			int begin = 0;
			int end;
			if (line.startsWith("Health info "))
				begin = 12/* "Health info ".length */;

			while (begin != -1 && begin < line.length()) {
				end = line.indexOf("=", begin);
				String key = line.substring(begin, end).trim();
				if (COLUMNS.containsKey(key))
					key = COLUMNS.get(key);

				begin = end + 1;
				if (line.charAt(begin) == '"') {
					int vbegin = ++begin;
					while (true) {
						end = line.indexOf("\"", vbegin);
						if (end == -1) {
							begin--;
							end = line.length();
							break;
						}

						int i;
						for (i = 1; i < end; i++) {
							if (line.charAt(end - i) != '\\') {
								break;
							}
						}

						if (i % 2 == 1)
							break;
						else {
							vbegin = end + 1;
							continue;
						}
					}
				} else {
					end = line.indexOf(" ", begin);
				}

				String value = line.substring(begin, end);
				m.put(key.toLowerCase(), value);
				begin = end + 1;
			}

			return m;
		} catch (Throwable t) {
			return log;
		}
	}
}
