package org.araqne.logparser.krsyslog.cyberoam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.log.api.FieldDefinition;
import org.araqne.log.api.V1LogParser;

public class CyberoamParser extends V1LogParser {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(CyberoamParser.class);

	private static final List<FieldDefinition> fields;

	static {
		fields = new ArrayList<FieldDefinition>();
		addField("date", "string");
		addField("time", "string");
		addField("timezone", "string");
		addField("device_name", "string");
		addField("device_id", "string");
		addField("log_id", "string");
		addField("log_type", "string");
		addField("log_component", "string");
		addField("log_subtype", "string");
		addField("status", "string");
		addField("priority", "string");
		addField("duration", "string");
		addField("firewall_rule_id", "string");
		addField("user_name", "string");
		addField("user_group", "string");
		addField("iap ", "string");
		addField("ips_policy_id", "string");
		addField("appfilter_policy_id", "string");
		addField("application", "string");
		addField("in_interface", "string");
		addField("out_interface", "string");
		addField("src_ip", "string");
		addField("src_mac", "string");
		addField("src_country_code", "string");
		addField("dst_ip", "string");
		addField("dst_country_code", "string");
		addField("protocol", "string");
		addField("src_port", "string");
		addField("dst_port", "string");
		addField("icmp_type", "string");
		addField("icmp_code", "string");
		addField("sent_pkts", "string");
		addField("received_pkts", "string");
		addField("sent_bytes", "string");
		addField("recv_bytes", "string");
		addField("trans_src_ ip", "string");
		addField("trans_src_port", "string");
		addField("trans_dst_ip", "string");
		addField("trans_dst_port", "string");
		addField("srczonetype", "string");
		addField("dstzonetype", "string");
		addField("dir_disp", "string");
		addField("connection_event", "string");
		addField("conn_id ", "string");
		addField("vconn_id", "string");
	}

	private static void addField(String name, String type) {
		fields.add(new FieldDefinition(name, type));
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

			// remove unused tag(e.g. <420>)
			int b = 0;
			int e = line.indexOf(">") + 1;
			StringBuilder builder = new StringBuilder(line);
			builder.delete(b, e);

			while ((e = builder.indexOf("=", b)) > 0) {
				String key = builder.substring(b, e).trim();
				if (key.equals("src_mac")) {
					String value = builder.substring(e + 1, e + 18);
					m.put(key, value);
					b = e + 19;
				} else {
					if (builder.charAt(e + 1) == '"') {
						String value = "";
						int i = e + 2;

						while (true) {
							char ch = builder.charAt(i);
							try {
								if (ch == '"' && builder.charAt(i + 1) == ' ') {
									value = builder.substring(e + 2, i);
									break;
								}
								i++;
							} catch (IndexOutOfBoundsException ex) {
								value = builder.substring(e + 2, i);
								break;
							}
						}

						m.put(key, value);
						b = i + 2;
					} else {
						int endPos = builder.indexOf(" ", e + 1);
						String value;
						if (endPos == -1) {
							value = builder.substring(e + 1);
							m.put(key, value);
							break;
						} else {
							value = builder.substring(e + 1, endPos);
							m.put(key, value);
							b = endPos + 1;
						}
					}
				}
			}

			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne log api: cannot parse cyberoam format - line [{}]", line);
			return params;
		}
	}
}
