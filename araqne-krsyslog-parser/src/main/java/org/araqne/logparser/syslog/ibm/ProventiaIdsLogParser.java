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
package org.araqne.logparser.syslog.ibm;

import java.util.Map;
import java.util.StringTokenizer;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kyun
 */
public class ProventiaIdsLogParser extends V1LogParser {
	private final Logger slog = LoggerFactory.getLogger(ProventiaIdsLogParser.class);

	private static final String[] from = new String[] {
		"1.3.6.1.2.1.1.3.0", 
		"1.3.6.1.6.3.1.1.4.1.0", 
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.1.0",
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.2.0", 
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.3.0",
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.4.0", 
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.5.0", 
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.6.0", 
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.7.0",
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.8.0", 
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.9.0",  
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.10.0", 
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.11.0"
	};

	private static final String[] to = new String[] {
		"sysUpTime", "snmptrapOID", "signature", "time", "protocol",
		"srcip", "dstip", "ICMPType", "ICMPCode", "srcport",
		"dstport", "ActionList", "extra"
		};

	@Override
	public Map<String, Object> parse(Map<String, Object> log) {
		rename(log);
		String line = (String) log.get("extra");
		if (line == null)
			return log;
		try {
			StringTokenizer tok = new StringTokenizer(line, ";");
			while (tok.hasMoreTokens()) {
				String token = tok.nextToken().trim();
				if(token.charAt(0) == ':')
					token = token.substring(1);
				int del = token.indexOf(":");
				if(del == -1)
					continue;
				String key = token.substring(0,del).trim();
				String value = token.substring(del+1).trim();
				if(key.equals("Protocol Name"))
					log.put("proto_name", value);
				else if(key.equals("URL"))
					log.put("url", value);
				else if(key.equals("server"))
					log.put("server", value);
				else if(key.equals("event-type"))
					log.put("event_type", value);
			}
			return log;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne syslog parser: ibm proventia ids parse error - [" + line + "]", t);
			return log;
		}
	}

	private void rename(Map<String, Object> m) {
		/*if(from.length != to.length)
			return;
		*/
		for(int i = 0; i < from.length; i++)
			if(m.containsKey(from[i]))
				m.put(to[i], m.remove(from[i]));
	}
		
}
