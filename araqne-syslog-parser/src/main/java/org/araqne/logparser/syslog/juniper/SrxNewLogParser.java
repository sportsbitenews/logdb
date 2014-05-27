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

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kyun
 */

public class SrxNewLogParser extends V1LogParser {
	private final Logger logger = LoggerFactory.getLogger(SrxNewLogParser.class.getName());

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		Map<String, Object> m = new HashMap<String, Object>();
		String line = (String) params.get("line");
		if (line == null)
			return null;

		Scanner s = new Scanner(line);
		try {
			s.useDelimiter(" +");

			String first = s.next();
			if (first.matches("[0-9]+")) {
				// date
				m.put("start_time", s.next());
				//s.next();
				
				m.put("device_id", s.next());
				s.next();
				s.next();
				String logtype = s.next();
				m.put("action", logtype);//logtype.substring("RT_FLOW_SESSION_".length()).toLowerCase());
				while (s.hasNext()) {
					String token = s.next();
					while (s.hasNext() && !token.startsWith("[") && !token.endsWith("]") && !token.endsWith("\""))
						token += " " + s.next();
					if (token.startsWith("["))
						continue;
					else if (token.endsWith("]"))
						token = token.substring(0, token.length() - 1);
					parseKeyValue(m, token);
				}
			} else {
				// day
				s.next();
				// time
				s.next();
				// type
				s.next();
				String logtype = s.next();
				s.next();
				s.next();

				if (logtype.equals("RT_FLOW_SESSION_CREATE:")) {
					m.put("action", "create");
					parseCommon(m, s);
				} else if (logtype.equals("RT_FLOW_SESSION_CLOSE:")) {
					m.put("action", "close");

					String reason = s.next();
					if (!reason.endsWith(":"))
						reason += " " + s.next();

					m.put("reason", reason.substring(0, reason.length() - 1));
					parseCommon(m, s);
					parseStat(m, "sent", s.next());
					parseStat(m, "rcvd", s.next());

					m.put("elapsed_time", Long.valueOf(s.next()));
				} else if (logtype.equals("RT_FLOW_SESSION_DENY:")) {
					m.put("action", "deny");
					parseFlow(m, s);

					String token = s.next();
					int p1 = token.indexOf('(');
					String protocol = token.substring(0, p1);
					String icmpType = token.substring(p1 + 1, token.length() - 1);

					m.put("protocol", protocol);
					m.put("icmp_type", icmpType);
					m.put("policy", s.next());
					m.put("src_zone", s.next());
					m.put("dst_zone", s.next());
				} else
					return params;
			}
		} catch (Throwable t) {
			logger.warn("araqne syslog parser: juniper srx3400 parse error [" + line + "]", t);
			return params;
		} finally {
			s.close();
		}

		return m;
	}

	private void parseKeyValue(Map<String, Object> m, String token) {
		String[] t = token.split("=", 2);
		m.put(t[0], t[1].subSequence(1, t[1].length() - 1));
	}

	private void parseStat(Map<String, Object> m, String prefix, String source) {
		int p1 = source.indexOf('(');
		m.put(prefix + "_" + "pkts", Long.valueOf(source.substring(0, p1)));
		m.put(prefix + "_" + "bytes", Long.valueOf(source.substring(p1 + 1, source.length() - 1)));
	}

	private void parseCommon(Map<String, Object> m, Scanner s) {
		parseFlow(m, s);

		String natFlow = s.next();
		int p4 = natFlow.indexOf('/');
		int p5 = natFlow.indexOf('-', p4);
		int p6 = natFlow.indexOf('/', p5);

		m.put("nat_src_ip", natFlow.substring(0, p4));
		m.put("nat_src_port", Integer.valueOf(natFlow.substring(p4 + 1, p5)));
		m.put("nat_dst_ip", natFlow.substring(p5 + 2, p6));
		m.put("nat_dst_port", Integer.valueOf(natFlow.substring(p6 + 1)));
		m.put("src_nat_rule", s.next());
		m.put("dst_nat_rule", s.next());
		m.put("protocol", s.next());
		m.put("policy", s.next());
		m.put("src_zone", s.next());
		m.put("dst_zone", s.next());
		m.put("session_id", s.next());
	}

	private void parseFlow(Map<String, Object> m, Scanner s) {
		String flow = s.next();
		int p1 = flow.indexOf('/');
		int p2 = flow.indexOf('-', p1);
		int p3 = flow.indexOf('/', p2);

		m.put("src_ip", flow.substring(0, p1));
		m.put("src_port", Integer.valueOf(flow.substring(p1 + 1, p2)));
		m.put("dst_ip", flow.substring(p2 + 2, p3));
		m.put("dst_port", Integer.valueOf(flow.substring(p3 + 1)));
		m.put("service", s.next());
	}

}
