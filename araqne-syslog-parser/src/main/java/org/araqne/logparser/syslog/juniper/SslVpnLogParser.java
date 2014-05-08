/*
 * Copyright 2014 Eediom Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logparser.syslog.juniper;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kyun
 */
public class SslVpnLogParser extends V1LogParser {
	private final Logger slog = LoggerFactory.getLogger(SslVpnLogParser.class.getName());

	private enum FieldType {
		String, Integer, Date
	};

	private  SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);

	private static final String[] Keys = new String[] { "vendor_logtime", "hostname", "ip_user", "msg"};

	private static final FieldType[] Types = new FieldType[] { 
		FieldType.String, FieldType.String, FieldType.String, FieldType.String};


	@Override
	public Map<String, Object> parse(Map<String, Object> log) {

		String line = (String) log.get("line");
		if (line == null)
			return log;

		Map<String, Object> m = new HashMap<String, Object>();

		try {
	
			int i =4;// Keys.length;
			String[] tokens = new String[i];
			
			for (int j = 0; j < i -1; j++) {
				int pos = line.indexOf(" - ");
				tokens[j] = line.substring(0, pos);
				line = line.substring(pos + 3/*"_-_".length*/); 
			}
			
			tokens[i-1] = line;
	
			 i = 0;
			for (String s : tokens) {
				
				String token = s.trim();
				if(token.equals("")) { 
					i++;
					continue; 
				}

				String key = Keys[i];
				FieldType type = Types[i];

				if(i == 0){
					String vendor = token.substring(0, token.indexOf(':'));
					m.put("vendor", vendor);

					String date = token.substring(token.indexOf(':') + 1).trim();
					m.put("logtime", format.parse(date));

				}else if(i == 2){
					String ip = token.substring(token.indexOf('[') + 1, token.indexOf(']'));
					m.put("ip", ip);

					String user = token.substring(token.indexOf(']') + 1).trim();
					m.put("user", user);

				}else {				
					if (type == FieldType.Integer)
						m.put(key, Integer.valueOf(token));
					else if (type == FieldType.Date)
						m.put(key, format.parse(token));
					else
						m.put(key, token);
				}
					i++;
			
			}
			
			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne syslog parser : junifer ssl parse error -[" + line + "]", t);
			return log;
		}
	}
}


