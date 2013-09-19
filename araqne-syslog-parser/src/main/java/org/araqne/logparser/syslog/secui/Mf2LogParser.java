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
package org.araqne.logparser.syslog.secui;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

/**
 * @since 1.9.2
 * @author xeraph
 * 
 */
public class Mf2LogParser extends V1LogParser {

	private static HashSet<String> numFields = new HashSet<String>();

	static {
		numFields.add("duration");
		numFields.add("src_port");
		numFields.add("dst_port");
		numFields.add("packets_forward");
		numFields.add("packets_backward");
		numFields.add("bytes_forward");
		numFields.add("bytes_backward");
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;

		int b = line.indexOf('[');
		if (b < 0)
			return params;

		int e = line.indexOf(']', b);
		String type = line.substring(b + 1, e);

		b = line.indexOf('[', e + 1);
		e = line.indexOf(']', b + 1);
		String device = line.substring(b + 1, e);

		b = e + 2;

		Map<String, Object> m = new HashMap<String, Object>();
		if (type.equals("fw4_allow")) {
			b = e + 2;

			int end = line.length();

			while (b < end) {
				e = line.indexOf('=', b);
				String key = line.substring(b, e);

				b = e + 1;
				if (line.charAt(b) == '"') {
					b++;
					e = line.indexOf('"', b);
					String value = line.substring(b, e);

					if (numFields.contains(key)) {
						long l = Long.valueOf(value);
						if (Integer.MIN_VALUE <= l && l <= Integer.MAX_VALUE)
							m.put(key, (int) l);
						else
							m.put(key, l);
					} else
						m.put(key, value);
					b = e + 2;
				} else {
					e = line.indexOf(' ', b);
					if (b != e) {
						if (key.equals("flag_record"))
							e = line.indexOf(" te", e);
						else if (key.equals("fw_rule_id"))
							e = line.indexOf(" nat_rule_id=");

						if (e > 0) {
							String value = line.substring(b, e);

							if (value.equals("-"))
								m.put(key, null);
							else {
								if (numFields.contains(key)) {
									long l = Long.valueOf(value);
									if (Integer.MIN_VALUE <= l && l <= Integer.MAX_VALUE)
										m.put(key, (int) l);
									else
										m.put(key, l);
								} else {
									m.put(key, value);
								}
							}
							b = e + 1;
						} else
							break;
					} else {
						m.put(key, null);
						b = e + 2;
					}
				}
			}

		} else {
			e = line.indexOf(',', e + 1);
			String eventAt = line.substring(b, e);

			m.put("msg", line.substring(e + 1));
			m.put("event_at", eventAt);
		}

		m.put("type", type);
		m.put("device", device);
		return m;
	}

}
