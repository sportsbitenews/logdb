package org.araqne.logparser.syslog.forescout;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

public class CounterActLogParser extends V1LogParser {
	private static final String[] LOG_TYPES = { "Port bite", "Scan event", "Uptime", "System statistics", "NAC Policy Log",
			"Application status", "Log", "User admin changed Configuration", "User admin changed network_policy",
			"User admin changed Action", "Block Event" };
	private static final Map<String, String> COLUMNS = new HashMap<String, String>();

	static {
		COLUMNS.put("Source:", "source_client");
		COLUMNS.put("Rule:", "nac_rule");
		COLUMNS.put("Details:", "nac_log_detail");
		COLUMNS.put("Reason:", "nac_reason");
		COLUMNS.put("Destination:", "destination_client");
		COLUMNS.put("CPU usage:", "cpu_use_percent");
		COLUMNS.put("Available memory :", "available_memory_kb");
		COLUMNS.put("Used memory:", "used_memory_kb");
		COLUMNS.put("Available swap", "available_swap_kb");
		COLUMNS.put("Used swap:", "used_swap_kb");
		COLUMNS.put("Application status:", "application_status");
		COLUMNS.put("Connected clients:", "connected_client");
		COLUMNS.put("Engine status:", "engine_status");
		COLUMNS.put("Attacked Services:", "attacked_service");
		COLUMNS.put("Installed Plugins:", "installed_plugin");
		COLUMNS.put("Stopped Plugins:", "stopped_plugin");
		COLUMNS.put("Assigned ips:", "assigned_ips");
		COLUMNS.put("Severity:", "severity");
		COLUMNS.put("Uptime", "nac_uptime_sec");
		COLUMNS.put("Log:", "nac_log");
		COLUMNS.put("Host:", "nac_host");
		COLUMNS.put("Target:", "nac_target");
		COLUMNS.put("Time ", "nac_time");
		COLUMNS.put("Service:", "nac_service");
		COLUMNS.put("Is Virtual Firewall blocking rule:", "is_virtual_firewall_blocking_rule");
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> log) {
		String line = (String) log.get("line");
		if (line == null)
			return log;

		try {
			Map<String, Object> m = new HashMap<String, Object>();

			int begin = 0;
			int end = line.indexOf(" ");

			String name = null;
			if (line.substring(4).contains("["))
				name = line.substring(4/* "NAC-".length */, line.indexOf("[", begin));

			if (name == null || name.contains(" ")) {
				end = line.indexOf(" ", 8); // skip 2 spaces
				m.put("time", line.substring(begin, end));
				begin = end + 1;

				end = line.indexOf(" ", begin);
				m.put("nac_name", line.substring(begin, end));
				begin = end + 1;

				m.put("description", line.substring(begin));

				return m;
			}

			m.put("nac_name", name);
			begin = end + 1;

			String logType = null;
			for (String type : LOG_TYPES) {
				if (line.startsWith(type, begin))
					logType = type;
			}
			if (logType == null)
				throw new IllegalArgumentException("invalid log string");

			m.put("nac_log_type", logType);
			if (!logType.equalsIgnoreCase("Uptime") && !logType.equals("Log") && !logType.equals("Application status"))
				begin = line.indexOf(" ", begin + logType.length()) + 1;

			String fieldName = null;
			end = line.length();
			for (String c : COLUMNS.keySet()) {
				if (m.containsKey(c))
					continue;
				int pos = line.indexOf(c, begin);
				if (pos != -1 && pos + c.length() < end) {
					fieldName = COLUMNS.get(c);
					end = pos + c.length();
				}
			}
			begin = end + 1;

			while (begin < line.length()) {
				String nextField = null;
				end = line.length();
				for (String c : COLUMNS.keySet()) {
					if (fieldName.equals(COLUMNS.get(c)))
						continue;
					int pos = line.indexOf(c, begin);
					if (pos != -1 && pos < end) {
						nextField = c;
						end = pos;
					}
				}

				if (nextField == null) {
					m.put(fieldName, getValue(logType, line.substring(begin)));
					break;
				}

				int i;
				for (i = 1; i < end - begin && line.charAt(end - i) == ' '; i++)
					;

				m.put(fieldName, getValue(logType, line.substring(begin, end - i)));
				fieldName = COLUMNS.get(nextField);
				begin = end + nextField.length() + 1;
			}

			return m;
		} catch (Throwable t) {
			return log;
		}
	}

	private Object getValue(String logType, String value) {
		if (logType.equals("Uptime") || logType.equals("System statistics")) {
			value = value.trim();
			int endIndex;
			for (endIndex = 0; endIndex < value.length() && '0' <= value.charAt(endIndex) && value.charAt(endIndex) <= '9'; endIndex++)
				;
			return Integer.parseInt(value.substring(0, endIndex));
		} else
			return value.trim();
	}
}