package org.araqne.logparser.krsyslog.citrix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.log.api.FieldDefinition;
import org.araqne.log.api.V1LogParser;

public class Mpx8400Parser extends V1LogParser {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(Mpx8400Parser.class);
	private final String delimiter = "- ";

	private static final List<FieldDefinition> fields;
	static {
		fields = new ArrayList<FieldDefinition>();

		addField("Access", "string");
		addField("Browser_type", "string");
		addField("Client_ip", "string");
		addField("Client_security_expression", "string");
		addField("Clientsecurityexpression", "string");
		addField("Compression_ratio_recv", "string");
		addField("Compression_ratio_send", "string");
		addField("Denied_by_policy", "string");
		addField("Denied_url", "string");
		addField("Destination", "string");
		addField("Duration", "string");
		addField("End_time", "string");
		addField("Failure_reason", "string");
		addField("Group(s)", "string");
		addField("Http_resources_accessed", "string");
		addField("Last_contact", "string");
		addField("License_limit", "string");
		addField("LogoutMethod", "string");
		addField("Nat_ip", "string");
		addField("NonHttp_services_accessed", "string");
		addField("Remote_host", "string");
		addField("SSLVPN_client_type", "string");
		addField("Source", "string");
		addField("Start_time", "string");
		addField("Total_TCP_connections", "string");
		addField("Total_UDP_flows", "string");
		addField("Total_bytes_recv", "string");
		addField("Total_bytes_send", "string");
		addField("Total_compressedbytes_recv", "string");
		addField("Total_compressedbytes_send", "string");
		addField("Total_policies_allowed", "string");
		addField("Total_policies_denied", "string");
		addField("User", "string");
		addField("Vserver", "string");
		addField("Xdata", "string");
		addField("Xdatalen", "string");
		addField("applicationName", "string");
		addField("connectionId", "string");
		addField("endTime", "string");
		addField("evaluatedto", "string");
		addField("startTime", "string");
		addField("username:domainname", "string");
	}

	@Override
	public List<FieldDefinition> getFieldDefinitions() {
		return fields;
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;
		try {
			Map<String, Object> m = new HashMap<String, Object>();
			int pos = line.indexOf(" : ");
			if (pos == -1)
				return params;

			int e = line.indexOf(" :", pos + 1);
			String prefix = line.substring(pos + 2, e);
			line = line.substring(e + 3);

			int b = 0;
			for (int i = 0; i < 3; ++i) {
				e = prefix.indexOf(" ", b);
				b = e + 1;
			}
			m.put("event_type", prefix.substring(0, e).trim());
			m.put("event_id", prefix.substring(e + 1));
			String[] keyValues = line.split(delimiter);

			if (m.get("event_type").equals("SSLVPN HTTPREQUEST")) {
				parseHttpRequest(m, keyValues);
			} else if (m.get("event_type").equals("SSLVPN Message")) {
				m.put("message_info", line.replace("\"", "").trim());
			} else {
				for (String keyValue : keyValues) {
					// remove whitespace
					if (keyValue.charAt(keyValue.length() - 1) == ' ')
						keyValue = keyValue.substring(0, keyValue.length() - 1);

					putKeyValue(m, keyValue);
				}
			}
			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne log api: cannot parse mpx8400 format - line [{}]", line);
			return params;
		}
	}

	private void parseHttpRequest(Map<String, Object> m, String[] keyValues) {
		for (String keyValue : keyValues) {
			// remove whitespace
			if (keyValue.charAt(keyValue.length() - 1) == ' ')
				keyValue = keyValue.substring(0, keyValue.length() - 1);

			int loopCount = 0;
			while (loopCount < 2) {
				String method;
				if(loopCount == 0)
					method = "GET";
				else
					method = "POST";

				int methodPos = keyValue.indexOf(method);
				if (methodPos != -1) {
					m.put("request_time", keyValue.substring(0, methodPos - 1));
					m.put(method.toLowerCase() + "_info", keyValue.substring(methodPos));
					return;
				}

				loopCount++;
			}

			int pos = keyValue.indexOf(" User");
			if (pos > 0) {
				String[] strs = keyValue.split(" : ");
				for (int i = 0; i < strs.length; ++i) {
					String str = strs[i];
					if (i == 0) {
						int firstPos = str.indexOf(" ", 1);
						int secondPos = str.indexOf(" ", firstPos + 1);
						m.put("url_info", str.substring(0, firstPos));
						m.put(str.substring(firstPos + 1, secondPos).toLowerCase(), str.substring(secondPos + 1).trim());
					} else {
						int firstPos = str.indexOf(" ");
						String key = str.substring(0, firstPos).toLowerCase();
						if (key.equals("group(s)"))
							key = "group_info";

						m.put(key, str.substring(firstPos + 1));
					}
				}

				continue;
			}

			putKeyValue(m, keyValue);
		}
	}

	private void putKeyValue(Map<String, Object> m, String keyValue) {
		int pos;
		pos = keyValue.indexOf(" \"");
		if (pos != -1) {
			String key = keyValue.substring(0, pos).toLowerCase().trim();
			if (key.equals("group(s)"))
				key = "group_info";

			m.put(key, keyValue.substring(pos + 2, keyValue.length() - 1));
			return;
		}

		pos = keyValue.indexOf(": ");
		if (pos != -1) {
			String key = keyValue.substring(0, pos).toLowerCase().trim();
			if (key.equals("group(s)"))
				key = "group_info";

			m.put(key, keyValue.substring(pos + 2));
			return;
		}

		// remove whitespace
		if (keyValue.charAt(0) == ' ')
			keyValue = keyValue.substring(1);

		pos = keyValue.indexOf(" ");
		if (pos != -1) {
			String key = keyValue.substring(0, pos).toLowerCase().trim();
			if (key.equals("source") || key.equals("destination")) {
				String value = keyValue.substring(pos + 1).trim();
				int p = value.indexOf(":");
				m.put(key + "_ip", value.substring(0, p));
				m.put(key + "_port", value.substring(p + 1));
				return;
			} else if (key.equals("group(s)"))
				key = "group_info";
			else
				m.put(key, keyValue.substring(pos + 1).trim());
		}
	}

	private static void addField(String name, String type) {
		fields.add(new FieldDefinition(name, type));
	}

}
