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
package org.araqne.logparser.syslog.hp;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;

import org.araqne.log.api.V1LogParser;
import org.araqne.logparser.syslog.riorey.RioreyDdosLogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * TippingPoint(IPS)
 * 
 * [샘플로그] Mar 18 19:12:23 sms-server 8 1 591aafad-b9f6-11e1-7265-be812d7c8911
 * 00000001-0001-0001-0001-000000007121 7121: TCP: Header Length Invalid, e.g.,
 * Fragroute 7121 ip 113.216.83.239 35385 203.235.200.42 80 1 3 1 MCIPS_1
 * 100739839 1395137543039 4301345
 * 
 * 
 * Time : Mar 18 19:12:23 Hostname : sms-server Action : 8 Serverity : 1 Policy
 * UUID : 591aafad-b9f6-11e1-7265-be812d7c8911 Signature UUID :
 * 00000001-0001-0001-0001-000000007121 Signature Name : 7121: TCP: Header
 * Length Invalid, e.g., Fragroute Signature Number : 7121 Protocol : ip
 * 
 * Source IP : 113.216.83.239 Source Port : 35385 Destination IP :
 * 203.235.200.42 Destination Port : 80 Hit Count : 1
 * 
 * Source Zone Name : 3 Destination Zone Name : 1 Device Name : MCIPS_1 Taxonomy
 * ID : 100739839 Event timestamp in milliseconds : 1395137543039 Additional
 * comments : 4301345 sequence number of the event :
 * 
 * @author kyun
 * @since 1.4.0
 */
public class TippingPointIpsLogParser extends V1LogParser {
	private final Logger slog = LoggerFactory.getLogger(RioreyDdosLogParser.class);

	private enum FieldType {
		String, Integer, Date
	};

	private static SimpleDateFormat format = new SimpleDateFormat("MMM dd HH:mm:ss", Locale.ENGLISH);

	private static final String[] Keys = new String[] { "time", "hostname", "action", "serverity", "policy_uuid", "sig_uuid",
			"sig_name", "sig_no", "protocol", "src_ip", "src_port", "dst_ip", "dst_port", "hit", "src_zone", "dst_zone",
			"device_name", "taxonomy_id", "event_timestamp", "comments", "event_seqno" };
	private static final FieldType[] Types = new FieldType[] { FieldType.String, FieldType.String, FieldType.Integer,
			FieldType.Integer, FieldType.String, FieldType.String, FieldType.String, FieldType.Integer, FieldType.String,
			FieldType.String, FieldType.Integer, FieldType.String, FieldType.Integer, FieldType.Integer, FieldType.String,
			FieldType.String, FieldType.String, FieldType.String, FieldType.String, FieldType.String, FieldType.String };

	@Override
	public Map<String, Object> parse(Map<String, Object> log) {

		String line = (String) log.get("line");
		if (line == null)
			return log;

		int i = 0;
		Map<String, Object> m = new HashMap<String, Object>();

		try {
			StringTokenizer tok = new StringTokenizer(line, "\t");

			while (tok.hasMoreTokens()) {
				if (i >= 21)
					break;
				String key = Keys[i];
				FieldType type = Types[i++];
				String token = tok.nextToken().trim();
				if (!token.equals("")) {
					if (type == FieldType.Integer)
						m.put(key, Integer.valueOf(token));
					else if (type == FieldType.Date)
						m.put(key, format.parse(token));
					else
						m.put(key, token);
				}
			}
			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne syslog parser: tippingpoint ips parse error - [{}]", line);
			return log;
		}
	}

}
