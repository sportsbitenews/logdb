package org.araqne.logparser.krsyslog.monitorlab;

import java.text.SimpleDateFormat;
import java.util.Map;

import org.araqne.log.api.DelimiterParser;
import org.araqne.log.api.V1LogParser;

public class WisgParser extends V1LogParser {
	private static final String[] systemLogHeaders = new String[] { "log_type", "time", "gateway", "cpu", "memory", "cps", "tps",
			"inbound_kbyte_persec", "outbound_kbyte_persec", "inbound_pps", "outbound_pps" };

	private static final String[] commonLogHeaders = new String[] { "time", "client_ip", "client_port", "server_ip",
			"server_port", "gateway", "detect_classification", "rule_id", "detect_base", "detect_result", "risk_level",
			"protocol", "host", "request_length", "request_data" };

	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(WisgParser.class);

	private DelimiterParser systemLogParser;
	private DelimiterParser commonLogParser;

	public WisgParser() {
		systemLogParser = new DelimiterParser("|", systemLogHeaders);
		commonLogParser = new DelimiterParser("|", commonLogHeaders);
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;

		try {
			String firstStr = line.substring(0, 14);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			try {
				sdf.parse(firstStr);

				Map<String, Object> m = commonLogParser.parse(params);
				return m;
			} catch (Exception e) {
				Map<String, Object> m = systemLogParser.parse(params);
				return m;
			}
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne-krsyslog-parser: cannot parse monitorlab wisg format - line [{}]", line);
			return params;
		}
	}
}
