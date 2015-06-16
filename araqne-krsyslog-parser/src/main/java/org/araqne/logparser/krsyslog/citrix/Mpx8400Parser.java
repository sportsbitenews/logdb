package org.araqne.logparser.krsyslog.citrix;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.log.api.FieldDefinition;
import org.araqne.log.api.V1LogParser;

public class Mpx8400Parser extends V1LogParser {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(Mpx8400Parser.class);
	private final String delimiter = "-";

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
			String[] keyValues = line.split(delimiter);

			for (String keyValuePair : keyValues) {
				Map<Integer, String> indexMap = new HashMap<Integer, String>();

				for (int i = 0; i < fields.size(); ++i) {
					int pos = 0;
					String keyField = fields.get(i).getName();
					if ((pos = keyValuePair.indexOf(keyField)) != -1)
						indexMap.put(pos, keyField);
				}

				List<Integer> indexes = new ArrayList<Integer>(indexMap.keySet());
				Collections.sort(indexes);

				int i = 0;
				while (i < indexes.size()) {
					int b = indexes.get(i);
					int keyFieldLength = indexMap.get(b).length();
					int boundary = b + keyFieldLength;
					if (i + 1 < indexes.size()) {
						int e = indexes.get(i + 1);
						m.put(keyValuePair.substring(b, boundary), keyValuePair.substring(boundary, e));
					} else {
						m.put(keyValuePair.substring(b, boundary), keyValuePair.substring(boundary));
					}
					i++;
				}
			}
			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne log api: cannot parse mpx8400 format - line [{}]", line);
			return params;
		}
	}

	private static void addField(String name, String type) {
		fields.add(new FieldDefinition(name, type));
	}

}
