/*
 * Copyright 2015 Eediom Inc.
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

public class MfiLogParser extends V1LogParser {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(Mf2LogParser.class.getName());

	private static Map<String, String[]> typeFieldMap = new HashMap<String, String[]>();

	public enum Mode {
		CSV, TSV
	};

	private Mode mode;

	static void add(String type, String fields) {
		String[] tokens = fields.split(",");
		typeFieldMap.put(type, tokens);
	}

	static {
		add("event", "start_time,end_time,machine,rule_name,profile,pdomain,bytes,packets,action,priority");
		add("deny",
				"timestamp,machine,rule_name,profile,pdomain,src_ip,dst_ip,interface,src_port,dst_port,protocol,ip_flag,tcp_flag,ttl,bytes,packets,priority");
		add("tot_traffic_rcv",
				"timestamp,machine,in_pps_tot,in_pps_detect,in_pps_acl,in_pps_system,out_pps_tot,out_pps_detect,out_pps_acl,out_pps_system,in_bps_tot,in_bps_detect,in_bps_acl,in_bps_system,out_bps_tot,out_bps_detect,out_bps_acl,out_bps_system");
		add("pdomain_traffic_rcv",
				"timestamp,machine,pdomain,in_pps_tot,in_pps_detect,in_pps_acl,in_pps_system,out_pps_tot,out_pps_detect,out_pps_acl,out_pps_system,in_bps_tot,in_bps_detect,in_bps_acl,in_bps_system,out_bps_tot,out_bps_detect,out_bps_acl,out_bps_system");
		add("segment_traffic_rcv",
				"timestamp,machine,segment,in_pps_tot,in_pps_detect,in_pps_acl,in_pps_system,out_pps_tot,out_pps_detect,out_pps_acl,out_pps_system,in_bps_tot,in_bps_detect,in_bps_acl,in_bps_system,out_bps_tot,out_bps_detect,out_bps_acl,out_bps_system");
		add("traffic_learning",
				"timestamp,machine,pdomain,total_pps,total_bps,tcp_pps,udp_pps,icmp_pps,other_pps,tcp_bps,udp_bps,icmp_bps,other_bps");
		add("traffic_acl", "timestamp,machine,src_ip,dst_ip,acl_id,dst_port,protocol,action,packets,bytes");
		add("traffic_qos", "timestamp,machine,queue_id,interface,total_bps,accept_pps,drop_pps,accept_rate,drop_rate");
		add("traffic_oversub",
				"timestamp,machine,in_drop_pps,in_bypass_pps,in_drop_bps,in_bypass_bps,out_drop_pps,out_bypass_pps,out_drop_bps,out_bypass_bps");
		add("interface",
				"timestamp,machine,segment,interface,bypass,llcf,autoneg,link,duplex,speed,rx_packet,rx_error,rx_dropped,rx_overruns,rx_frame,rx_bytes,tx_packet,tx_error,tx_dropped,tx_overruns,tx_frame,tx_bytes");
		add("resource", "timestamp,machine,core_num,cpu_usage,mem_total,disk_total,mem_usage,disk_usage,cpu1_temp,cpu2_temp");
		add("system_event", "timestamp,machine,event_type,msg");
		add("audit", "timestamp,machine,admin_ip,cmd_code,result_code,admin_level,menu_id,admin_id,parameter,fail_msg,diff_msg");
		add("alert", "timestamp,machine,alert_type,alert_level,msg");
	}

	public MfiLogParser(Mode mode) {
		this.mode = mode;
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return params;

		try {
			Map<String, Object> m = new HashMap<String, Object>();

			// parse header
			int b = line.indexOf('<');
			int e = line.indexOf('>', b);
			if (b >= 0 && e > 0) {
				String facility = line.substring(b + 1, e);
				m.put("facility", facility);
				b = e = e + 3;
			} else {
				b = e = 2;
			}
			e = line.indexOf(' ', e + 1);
			String dateTime = line.substring(b, e);

			b = line.indexOf('[', e + 1);
			e = line.indexOf(']', e + 1);
			String logType = line.substring(b + 1, e);

			b = line.indexOf('[', e + 1);
			e = line.indexOf(']', e + 1);
			String fromIp = line.substring(b + 1, e);

			m.put("datetime", dateTime);
			m.put("log_type", logType);
			m.put("from_ip", fromIp);

			int delimiter;
			if (mode == Mode.CSV) {
				delimiter = ',';
			} else {
				delimiter = '\t';
			}

			String[] fields = typeFieldMap.get(logType);
			parse(line, m, fields, e, delimiter);
			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne krsyslog parser: cannot parse log [" + line + "]", t);

			return params;
		}
	}

	private void parse(String line, Map<String, Object> m, String[] fields, int e, int delimiter) {
		int b = e + 2;
		if (b < 0)
			return;

		int index = 0;
		try {
			while ((e = line.indexOf(delimiter, b + 1)) != -1) {
				String content = line.substring(b, e);
				m.put(fields[index], content);

				b = e + 1;
				index++;
			}

			String content = line.substring(b);
			m.put(fields[index], content);
		} catch (IndexOutOfBoundsException e1) {
			throw e1;
		}
	}
}