package org.araqne.logparser.syslog.symantec;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.log.api.FieldDefinition;
import org.araqne.log.api.V1LogParser;

public class ProtectionEngineLogParser extends V1LogParser {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(ProtectionEngineLogParser.class);
	private static final List<FieldDefinition> fields;
	private final String delimiter = "\\|";

	static {
		fields = new ArrayList<FieldDefinition>();
		addField("event", "string");
		addField("event_severity_level", "string");
		addField("url", "string");
		addField("file_name", "string");
		addField("file_status", "string");
		addField("component_name", "string");
		addField("component_disposition", "string");
		addField("scan_rule", "string");
		addField("virus_name", "string");
		addField("virus_id", "string");
		addField("mail_policy_violation", "string");
		addField("container_violation", "string");
		addField("file_attribute_violation", "string");
		addField("matching_url", "string");
		addField("categories", "string");
		addField("ddr_score ", "string");
		addField("scan_duration(sec)", "string");
		addField("connect_duration(sec)", "string");
		addField("product_version", "string");
		addField("decomposer_version", "string");
		addField("virus_definitions", "string");
		addField("symantec_url_definitions", "string");
		addField("ddr_definitions_version", "string");
		addField("previous_virus_definitions", "string");
		addField("previous_symantec_url_definitions", "string");
		addField("previous_ddr_definitions_version", "string");
		addField("definitions", "string");
		addField("outbreak_events", "string");
		addField("outbreak_interval(sec)", "string");
		addField("error_message", "string");
		addField("feature_name", "string");
		addField("expiration_date", "string");
		addField("scanner", "string");
		addField("result_id", "string");
		addField("symantec_protection_engine_threshold_queue_size", "string");
		addField("symantec_protection_engine_number_of_queued_items", "string");
		addField("client_sid", "string");
		addField("client_computer", "string");
		addField("client_ip", "string");
		addField("server_ip", "string");
		addField("subscriber_id", "string");
		addField("logging_destination", "string");
		addField("symantec_protection_engine_ip_address", "string");
		addField("symantec_protection_engine_port_number", "string");
		addField("uptime(in seconds)", "string");
		addField("filer_ip", "string");
		addField("rpc_request_id", "string");
		addField("non_viral_threat_name", "string");
		addField("non_viral_threat_id", "string");
		addField("security_risk_definitions", "string");
		addField("statistics_string", "string");
		addField("start_time", "string");
		addField("end_time", "string");
		addField("configured_scan_requests_per_second", "string");
		addField("scan_requests_per_second", "string");
		addField("update_method", "string");
		addField("security_risk_category", "string");
		addField("caic_url_definitions", "string");
		addField("previous_caic_url_definitions", "string");
		addField("user_login_name", "string");
		addField("console_ip", "string");
		addField("group_name", "string");
		addField("warning_message", "string");
		addField("uber_category", "string");
		addField("sub_category_id", "string");
		addField("sub_category_name", "string");
		addField("sub_category_description", "string");
		addField("cumulative_risk_rating", "string");
		addField("performance_impact", "string");
		addField("privacy_impact", "string");
		addField("ease_of_removal", "string");
		addField("stealth", "string");

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
			String[] s = line.split(delimiter);
			Map<String, Object> m = new HashMap<String, Object>();
			Date d = new Date(Long.parseLong(s[0]) * 1000L);

			m.put("datetime", d);
			m.put("event", s[1]);

			for(int i = 2; i < s.length; i += 2) {
				FieldDefinition fd = fields.get(Integer.parseInt(s[i]) - 1);
				m.put(fd.getName(), s[i + 1]);
			}
			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne log api: cannot parse mpx8400 format - line [{}]", line);
			return params;
		}
	}

}