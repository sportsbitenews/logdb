package org.araqne.logparser.krsyslog.monitorapp;

import java.util.HashMap;
import java.util.Map;

import org.araqne.codec.UnsupportedTypeException;
import org.araqne.log.api.V1LogParser;

public class WisgParser extends V1LogParser {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(WisgParser.class);

	private final static String[] detectFields = {"mgmt_ip", "version", "time", "detect_code_num",
		"detect_type", "rule_name", "client_ip","client_port", "server_ip", 
		"server_port", "detect_contents", "action", "severity", "protocol", 
		"host", "request_len", "request_data" };
	
	private final static String[] systemFields = {"mgmt_ip", "version", "time", "cpu_avg",
		"mem_avg", "disk_avg", "link_status","open_connection", "cps", 
		"tps", "bps", "httpgw_status" };
	

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;
		try {
			Map<String, Object> m = new HashMap<String, Object>();
			
			String type;
			char delim;
			if(line.startsWith("DETECT")){
				type = "DETECT";
				delim = line.charAt(6);
			}else 	if(line.startsWith("SYSTEM")){
				type = "SYSTEM";
				delim = line.charAt(6);
			} else
				throw new UnsupportedTypeException(line);
			
			m.put("log_type", type);
			int beginIndex = 7;
			int endIndex = 0;
			if(type.equals("DETECT")){
				for(String fields: detectFields) {
					endIndex = line.indexOf(delim, beginIndex);
					if(endIndex < 0)
						endIndex = line.length();
					m.put(fields, line.substring(beginIndex, endIndex).trim());
					beginIndex = endIndex + 1;
				}
			}else if(type.equals("SYSTEM")) {
				for(String fields: systemFields) {
					endIndex = line.indexOf(delim, beginIndex);
					if(endIndex < 0)
						endIndex = line.length();
					m.put(fields, line.substring(beginIndex, endIndex).trim());
					beginIndex = endIndex + 1;
				}
			}else {
				throw new UnsupportedTypeException(line);
			}

			return m;

		} catch (Throwable t) {
			if (slog.isDebugEnabled()){
				slog.debug("araqne log api: cannot parse ICTIS iWall - line [{}]", line);
				slog.debug("detail", t);
			}
			return params;
		}
	}
}
