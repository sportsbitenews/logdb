package org.araqne.logparser.krsyslog.secuwiz;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecuwizSslLogParser extends V1LogParser {
	private final Logger slog = LoggerFactory.getLogger(SecuwizSslLogParser.class);

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return null;

		try {
			SimpleDateFormat sdf = new SimpleDateFormat("MMM dd HH:mm:ss", Locale.ENGLISH);
			Map<String, Object> m = new HashMap<String, Object>();
			int msgIndex;
			msgIndex = parseHeader(line, m, sdf) + 1;

			String logType = (String) m.get("log_type");

			String data = line.substring(msgIndex);
			String[] tokens = tokenizeLine(data.trim(), ",");
			if (logType.equals("logger"))
				parseLoggerLog(tokens, m);
			else if (logType.equals("access_log"))
				parseAccessLog(tokens, m);
			else if (logType.equals("system_log")) {
				m.put("message", data);
			}

			return m;
		} catch (Throwable t) {
			slog.debug("araqne syslog parser: cannot parse secuwiz ssl vpn log => " + line, t);
			return null;
		}
	}

	private void parseAccessLog(String[] tokens, Map<String, Object> m) {
		for (int i = 0; i < tokens.length; i++) {
			String keyValue = tokens[i];
			String[] split = tokenizeLine(keyValue, "=");
			String key = split[0];
			if (key.equals("serverIP"))
				key = "server_ip";
			else if (key.equals("ID"))
				key = "id";
			else if (key.equals("clientIP"))
				key = "client_ip";
			else if (key.equals("natIP"))
				key = "nat_ip";
			String value = split[1];
			m.put(key, value.isEmpty() ? null : value);
		}
	}

	private void parseLoggerLog(String[] tokens, Map<String, Object> m) {
		List<String> columns = Arrays.asList("connect_id", "user_virtual_ip", "login_out_tag", "user_real_ip");
		for (int i = 0; i < columns.size(); i++) {
			String value = tokens[i];
			m.put(columns.get(i), value.isEmpty() ? null : value);
		}
	}

	private int parseHeader(String line, Map<String, Object> m, SimpleDateFormat sdf) throws ParseException {
		List<String> columns = Arrays.asList("sys_svr_time", "source_ip", "vpn_time", "hostname", "log_type");
		int s = 0;
		int e = 0;
		for (String column : columns) {
			Object value = null;
			if (column.endsWith("_time")) {
				e = s + 15;
				Date d = sdf.parse(line.substring(s, e));
				Calendar c = Calendar.getInstance();
				int year = c.get(Calendar.YEAR);
				c.setTime(d);
				c.set(Calendar.YEAR, year);
				value = c.getTime();
			} else if (column.equals("log_type")) {
				e = line.indexOf(":", s);
				value = line.substring(s, e);
			} else {
				e = line.indexOf(" ", s);
				value = line.substring(s, e);
			}

			m.put(column, value);
			s = e + 1;
		}
		return e;
	}

	private String[] tokenizeLine(String line, String delimiter) {
		int last = 0;
		List<String> tokenizedLine = new ArrayList<String>(32);
		while (true) {
			int p = line.indexOf(delimiter, last);

			String token = null;
			if (p >= 0)
				token = line.substring(last, p);
			else
				token = line.substring(last);

			tokenizedLine.add(token);

			if (p < 0)
				break;
			last = ++p;
		}

		return tokenizedLine.toArray(new String[0]);
	}
}
