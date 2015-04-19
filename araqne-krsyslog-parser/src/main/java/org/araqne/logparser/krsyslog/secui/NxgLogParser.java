/*
 * Copyright 2013 Eediom Inc.
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
package org.araqne.logparser.krsyslog.secui;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.DelimiterParser;
import org.araqne.log.api.V1LogParser;

public class NxgLogParser extends V1LogParser {

	private DelimiterParser p = new DelimiterParser(",", null);

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null || line.isEmpty())
			return params;

		int b = 0;
		if (line.charAt(0) == '<') {
			b = line.indexOf('>') + 1;
		}

		HashMap<String, Object> m = new HashMap<String, Object>();
		int e = line.indexOf(' ');

		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String type = line.substring(b, e);
		if (type.equals("[LOG_ADMITTED]")) {
			m.put("type", "allow");

			Date d = df.parse(line, new ParsePosition(e + 1));
			if (d != null)
				m.put("_time", d);

			DelimiterParser p = new DelimiterParser(",", null);
			Map<String, Object> dm = p.parse(params);

			m.put("ui", dm.get("column1"));
			m.put("src_ip", dm.get("column2"));
			m.put("src_port", Integer.valueOf((String)dm.get("column3")));
			m.put("protocol", dm.get("column4"));
			m.put("dst_ip", dm.get("column5"));
			m.put("dst_port", Integer.valueOf((String)dm.get("column6")));
			m.put("send_byte", dm.get("column7"));
			m.put("rcv_bytes", dm.get("column8"));
			m.put("duration", dm.get("column9"));
			m.put("interface", dm.get("column10"));
		} else if (type.equals("[LOG_DENIED]")) {
			m.put("type", "deny");
			Date d = df.parse(line, new ParsePosition(e + 1));
			if (d != null)
				m.put("_time", d);

			Map<String, Object> dm = p.parse(params);

			m.put("rule_id", Integer.valueOf((String)dm.get("column1")));
			m.put("src_ip", dm.get("column2"));
			m.put("dst_ip", dm.get("column3"));
			m.put("protocol", dm.get("column4"));
			m.put("src_port", Integer.valueOf((String) dm.get("column5")));
			m.put("dst_port", Integer.valueOf((String) dm.get("column6")));
			m.put("denied_action", dm.get("column7"));
			m.put("count", Integer.valueOf((String)dm.get("column8")));
			m.put("interface", dm.get("column9"));
		} else if (type.equals("[LOG_NAT]")) {
			m.put("type", "nat");
			Date d = df.parse(line, new ParsePosition(e + 1));
			if (d != null)
				m.put("_time", d);

			Map<String, Object> dm = p.parse(params);
			String direction = (String) dm.get("column6");
			String ip = (String) dm.get("column1");
			String natIp = (String) dm.get("column3");
			int port = Integer.valueOf((String) dm.get("column2"));
			int natPort = Integer.valueOf((String) dm.get("column4"));

			m.put("direction", direction);

			if (direction.equals("Outbound")) {
				m.put("src_ip", ip);
				m.put("src_port", port);
				m.put("nat_src_ip", natIp);
				m.put("nat_src_port", natPort);
			} else {
				m.put("dst_ip", ip);
				m.put("dst_port", port);
				m.put("nat_dst_ip", natIp);
				m.put("nat_dst_port", natPort);
			}

			m.put("protocol", dm.get("column5"));

		} else if (type.equals("[LOG_AUDIT]")) {
			m.put("type", "audit");

			b = e + 1;
			e = line.indexOf(' ', b);
			m.put("level", line.substring(b, e));

			Date d = df.parse(line, new ParsePosition(e + 2));
			if (d != null)
				m.put("_time", d);

			m.put("msg", line.substring(e + 23));
		}

		return m;
	}
}
