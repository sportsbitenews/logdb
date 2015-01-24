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

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

/**
 * @since 1.9.2
 * @author xeraph
 * 
 */
public class Mf2LogParser extends V1LogParser {
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
		String fromIp = line.substring(b + 1, e);

		Map<String, Object> m = new HashMap<String, Object>();
		m.put("type", type);
		m.put("from_ip", fromIp);
		
		if(type.equals("fw4_allow")) {
			parseFw4AllowCsv(line, m, e);
		} else if(type.equals("fw4_deny")) {
			parseFw4DenyCsv(line, m, e);
		}
		
		return m;
	}
	
	private void parseFw4AllowCsv(String line, Map<String, Object> m, int e) {
		int loopCount = 0;
		int b = e + 2;
		while((e = line.indexOf(',', b + 1)) != -1) {
			String content = line.substring(b, e);
			
			switch(loopCount) {
			case 0:
				m.put("start_time", content);
				break;
			case 1:
				m.put("end_time", content);
				break;
			case 2: 
				m.put("duration", content);
				break;
			case 3:
				m.put("machine_name", content);
				break;
			case 4:
				m.put("fw_rule_id", content);
				break;
			case 5:
				m.put("nat_rule_id", content);
				break;
			case 6:
				m.put("src_ip", content);
				break;
			case 7:
				m.put("src_port", content);
				break;
			case 8:
				m.put("dst_ip", content);
				break;
			case 9:
				m.put("dst_port", content);
				break;
			case 10:
				m.put("protocol", content);
				break;
			case 11:
				m.put("ingres_if", content);
				break;
			case 12:
				m.put("tx_packets", content);
				break;
			case 13:
				m.put("rx_packets", content);
				break;
			case 14:
				m.put("tx_bytes", content);
				break;
			case 15:
				m.put("rx_bytes", content);
				break;
			case 16:
				m.put("fragment_info", content);
				break;
			case 17:
				m.put("flag_record", content);
				break;
			}
			
			b = e + 1;
			loopCount++;
		}
		
		String content = line.substring(b);
		m.put("terminate_reason", content);
	}
	
	private void parseFw4DenyCsv(String line, Map<String, Object> m, int e) {
		int loopCount = 0;
		int b = e + 2;
		while((e = line.indexOf(',', b + 1)) != -1) {
			String content = line.substring(b, e);
			
			switch(loopCount) {
			case 0:
				m.put("start_time", content);
				break;
			case 1:
				m.put("end_time", content);
				break;
			case 2: 
				m.put("duration", content);
				break;
			case 3:
				m.put("machine_name", content);
				break;
			case 4:
				m.put("fw_rule_id", content);
				break;
			case 5:
				m.put("nat_rule_id", content);
				break;
			case 6:
				m.put("src_ip", content);
				break;
			case 7:
				m.put("src_port", content);
				break;
			case 8:
				m.put("dst_ip", content);
				break;
			case 9:
				m.put("dst_port", content);
				break;
			case 10:
				m.put("protocol", content);
				break;
			case 11:
				m.put("ingres_if", content);
				break;
			case 12:
				m.put("packets", content);
				break;
			case 13:
				m.put("bytes", content);
				break;
			case 14:
				m.put("fragment_info", content);
				break;
			case 15:
				m.put("flag_record", content);
				break;
			}
			
			b = e + 1;
			loopCount++;
		}
		
		String content = line.substring(b);
		m.put("terminate_reason", content);
	}
}
