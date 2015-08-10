package org.araqne.logparser.syslog.forescout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
		COLUMNS.put("Destination:", "destination_client");
		COLUMNS.put("CPU usage:", "cpu_use_percent");
		COLUMNS.put("Available memory :", "available_memory_kb");
		COLUMNS.put("Used memory:", "used_memory_kb");
		COLUMNS.put("Available swap:", "available_swap_kb");
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
		COLUMNS.put("Host:", "block_host");
		COLUMNS.put("Target:", "block_target");
		COLUMNS.put("Time ", "block_time");
		COLUMNS.put("Service:", "block_service");
		COLUMNS.put("Is Virtual Firewall blocking rule:", "block_rule");
		COLUMNS.put("Reason:", "_reason");
		COLUMNS.put("Match:", "nac_match");
		COLUMNS.put("Category:", "nac_category");
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

			List<ColumnPosition> position = new ArrayList<ColumnPosition>();
			for (String c : COLUMNS.keySet()) {
				int pos = line.indexOf(c, begin);
				if (pos != -1) {
					position.add(new ColumnPosition(COLUMNS.get(c), pos, pos + c.length()));
				}
			}
			Collections.sort(position, new Comparator<ColumnPosition>() {
				@Override
				public int compare(ColumnPosition o1, ColumnPosition o2) {
					return o1.position - o2.position;
				}
			});

			if (position.size() == 0) {
				String value = line.substring(begin);
				if (!value.trim().isEmpty())
					m.put("line", value.trim());
				return m;
			}

			Iterator<ColumnPosition> iter = position.iterator();
			ColumnPosition current = iter.next();

			while (iter.hasNext()) {
				ColumnPosition next = iter.next();
				end = next.position;
				int i;
				for (i = 1; i < next.position - current.value && line.charAt(next.position - i) == ' '; i++)
					;

				name = current.name;
				if (name.equals("_reason"))
					name = logType.equals("Block Event") ? "block_reason" : "nac_reason";

				m.put(name, getValue(logType, line.substring(current.value, next.position - i)));
				if (name.equals("block_service")) {
					String value = (String) m.get("block_service");
					int pos = value.indexOf("/");
					if (pos != -1) {
						m.put("block_service", value.substring(0, pos));
						m.put("block_protocol", value.substring(pos + 1));
					}
				}
				current = next;
			}
			m.put(name, getValue(logType, line.substring(current.value)));

			if (m.containsKey("nac_rule")) {
				String value = (String) m.get("nac_rule");
				begin = value.indexOf("\"") + 1;
				if (begin != 0)
					end = value.lastIndexOf("\"");
				if (begin != 0 && end != -1)
					value = value.substring(begin, end);
				m.put("nac_rule", value);
			}

			return m;
		} catch (Throwable t) {
			t.printStackTrace();
			return log;
		}
	}

	private class ColumnPosition {
		private String name;
		private int position;
		private int value;

		public ColumnPosition(String name, int position, int value) {
			this.name = name;
			this.position = position;
			this.value = value;
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
