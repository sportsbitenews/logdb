package org.araqne.logarser.krsyslog.itcis;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

public class IwallFireWallParser extends V1LogParser {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(IwallFireWallParser.class);
	private final static String[] auditFields = {"log_time", "user", "user_ip", "authority", "main_category", "sub_category", "action", "result"};

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;
		try {
			Map<String, Object> m = new HashMap<String, Object>();
			
			int beginIndex = 0;
			int endIndex = 19;
			m.put("time", line.substring(beginIndex, endIndex));
	
			beginIndex = endIndex + 1;
			endIndex = line.indexOf(' ', beginIndex);
			m.put("machine_name", line.substring(beginIndex, endIndex));
			
			beginIndex = endIndex + 1;
			endIndex = line.indexOf(':', beginIndex);
			m.put("system_name", line.substring(beginIndex, endIndex));
			
			beginIndex = endIndex + 9; //": prefix=".length
			endIndex = line.indexOf(' ', beginIndex);
			m.put("prefix", line.substring(beginIndex, endIndex));
			
			beginIndex = endIndex + 6; //" type=".length
			endIndex = line.indexOf(' ', beginIndex);
			String type = line.substring(beginIndex, endIndex);
			m.put("type", type);
			
			if(type.equals("audit")){
				beginIndex = endIndex + 6; //" msg=\"".length
				endIndex = line.indexOf ('\"', beginIndex);
				String msg = line.substring(beginIndex, endIndex);
				m.put("msg", msg);
				
				beginIndex = 0;
				for(String fields: auditFields) {
					endIndex = msg.indexOf(';', beginIndex);
					if(endIndex < 0)
						endIndex = msg.length();
					m.put(fields, msg.substring(beginIndex, endIndex));
					beginIndex = endIndex + 1;
				}
			}else {
				int pos = endIndex + 1;
				int exPos = pos;
				String key = "";
				while( ++pos < line.length() ){
					if(line.charAt(pos) == '='){
						key = line.substring(exPos, pos);
						exPos = pos + 1;
					}else if(line.charAt(pos) == ' '  ){
						m.put(key, line.substring(exPos, pos));
						exPos = pos + 1;
					}
				}
				m.put(key, line.substring(exPos));
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
