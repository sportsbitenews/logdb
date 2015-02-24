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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.log.api.V1LogParser;

/**
 * @since 1.9.2
 * @author mindori
 * 
 */
public class Mf2LogParser extends V1LogParser {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(Mf2LogParser.class.getName());

	private static Map<String, List<String[]>> overlappedFieldMap = new HashMap<String, List<String[]>>();
	private static Map<String, String[]> typeFieldMap = new HashMap<String, String[]>();

	public enum Mode {
		CSV, TSV
	};

	private Mode mode;

	static void add(String type, String fields) {
		String[] tokens = fields.split(",");
		typeFieldMap.put(type, tokens);
	}

	static void addOverlapped(String type, String fields) {
		String[] tokens = fields.split(",");
		if (overlappedFieldMap.get(type) == null) {
			overlappedFieldMap.put(type, new ArrayList<String[]>());
		}

		List<String[]> l = overlappedFieldMap.get(type);
		l.add(tokens);
	}

	static {
		// 1.1 - 1.15
		add("ha_event", "time,machine_name,event_name,msg");
		add("audit", "time,machine_name,admin_id,admin_ip,admin_level,menu_path,command,result,fail_reason,difference");
		add("ha_traffic", "time,machine_name,tx_packets,rx_packets,tx_bytes,rx_bytes,traffictype");
		add("ha_status", "time,members,member_name,priority,member_status");
		add("mng_iap_interworking",
				"time,machine_name,sensor_ip,attacker_ip,attacker_port,victim_ip,victim_port,protocol,block_period,msg");
		add("mng_blacklist", "time,machine_name,src_ip,src_port,dst_ip,dst_port,protocol,block_period,packets,bytes,type,reason");
		add("mng_blacklist_ipv6",
				"time,machine_name,src_ip,src_port,dst_ip,dst_port,protocol,block_period,packets,bytes,type,reason");
		add("mng_line", "time,machine_name,interface,msg");
		add("mng_resource", "time,machine_name,cpu_cores,cpu_usages,memory_capacity,memory_usages,disk_capacity,disk_usages");
		add("mng_daemon", "time,machine_name,daemon_name,cpu_usages,virtual_memmory_usages,real_memory_usages");
		add("mng_if_traffic", "time,machine_name,interface,link_status,rx_frames,tx_frames,rx_bytes,tx_bytes");
		add("mng_oversubscription",
				"time,machine_name,incoming_bypass_pps,incoming_bypass_bps,incoming_drop_pps,incoming_drop_bps,outgoing_bypass_pps,outgoing_bypass_bps,outgoing_drop_pps,outgoing_drop_bps");
		add("mng_qos", "time,machine_name,queue_name,interface,use_bandwidth,allow_packets,allow_rate,loss_packets,loss_rate");
		add("mng_fqdn_object_management", "time,machine_name,object_name,before,after");
		add("mng_user_object_management", "time,machine_name,user_name,before,after");
		// 2.1 - 2.11
		add("fw4_allow",
				"start_time,end_time,duration,machine_name,fw_rule_id,nat_rule_id,src_ip,src_port,dst_ip,dst_port,protocol,ingres_if,tx_packets,rx_packets,tx_bytes,rx_bytes,fragment_info,flag_record,terminate_reason");
		add("fw4_deny",
				"start_time,end_time,duration,machine_name,fw_rule_id,nat_rule_id,src_ip,src_port,dst_ip,dst_port,protocol,ingres_if,packets,bytes,fragment_info,flag_record,terminate_reason");
		add("fw6_allow",
				"start_time,end_time,duration,machine_name,fw_rule_id,src_ip,src_port,dst_ip,dst_port,protocol,ingres_if,tx_packets,rx_packets,tx_bytes,rx_bytes,extented_header,fragment_info,flag_record,terminate_reason");
		add("fw6_deny",
				"start_time,end_time,duration,machine_name,fw_rule_id,src_ip,src_port,dst_ip,dst_port,protocol,ingres_if,packets,bytes,extented_header,fragment_info,flag_record,terminate_reason");
		add("nat_session",
				"start_time,end_time,machine_name,nat_rule_id,fw_rule_id,applied_if,src_ip,src_port,dst_ip,dst_port,protocol,src_ip_nat,dst_ip_nat,src_port_nat,dst_port_nat,packets,bytes");
		add("fw4_traffic", "time,machine_name,allow_packets,deny_packets,sessions,max_sessions,allow_bytes,deny_bytes");
		add("fw4_rule_traffic", "time,machine_name,fw_rule_id,action,packets,bytes,sessions,max_sessions");
		add("fw6_traffic", "time,machine_name,allow_packets,deny_packets,sessions,max_sessions,allow_bytes,deny_bytes");
		add("fw6_rule_traffic", "time,machine_name,fw_rule_id,action,packets,bytes,sessions,max_sessions");
		add("nat_traffic", "time,machine_name,packets,bytes,sessions,max_sessions");
		add("nat_rule_traffic", "time,machine_name,nat_rule_id,packets,bytes,sessions,max_sessions");
		// 3.1 - 3.9
		add("ips_ddos_detect",
				"start_time,machine_name,attack_name,domain_id,src_ip,src_port,dst_ip,dst_port,protocol,ip_flag,tcp_flag,src_mac,packets,bytes,action,dump_id");
		add("ips_ddos_incident", "start_time,end_time,machine_name,attack_name,priority,domain_id,packets,bytes,action");
		add("ips_ddos_traffic",
				"time,machine_name,incoming_pps,incoming_detect_pps,incoming_bps,incoming_detect_bps,outgoing_pps,outgoing_detect_pps,outgoing_bps,outgoing_detect_bps");
		addOverlapped(
				"ips_ddos_domain_traffic",
				"time,machine_name,domain_id,incoming_tcp,incoming_detect_tcp,incoming_udp,incoming_detect_udp,incoming_icmp,incoming_detect_icmp,incoming_etc,incoming_detect_etc,outgoing_tcp,outgoing_detect_tcp,outgoing_udp,outgoing_detect_udp,outgoing_icmp,outgoing_detect_icmp,outgoing_etc,outgoing_detect_etc");
		addOverlapped(
				"ips_ddos_domain_traffic",
				"time,machine_name,domain_id,incoming_64,incoming_detect_64,incoming_128,incoming_detect_128,incoming_256,incoming_detect_256,incoming_512,incoming_detect_512,incoming_1024,incoming_detect_1024,incoming_1518,incoming_detect_1518,outgoing_64,outgoing_detect_64,outgoing_128,outgoing_detect_128,outgoing_256,outgoing_detect_256,outgoing_512,outgoing_detect_512,outgoing_1024,outgoing_detect_1024,outgoing_1518,outgoing_detect_1518");
		addOverlapped(
				"ips_ddos_domain_traffic",
				"time,machine_name,domain_id,incoming_64,incoming_detect_64,incoming_128,incoming_detect_128,incoming_160,incoming_detect_160,incoming_192,incoming_detect_192,incoming_234,incoming_detect_234,incoming_256,incoming_detect_256,outgoing_64,outgoing_detect_64,outgoing_128,outgoing_detect_128,outgoing_160,outgoing_detect_160,outgoing_192,outgoing_detect_192,outgoing_234,outgoing_detect_234,outgoing_256,outgoing_detect_256");
		addOverlapped(
				"ips_ddos_domain_traffic",
				"time,machine_name,domain_id,incoming_syn,incoming_detect_syn,incoming_fin,incoming_detect_fin,incoming_rst,incoming_detect_rst,incoming_push,incoming_detect_push,incoming_ack,incoming_detect_ack,incoming_urg,incoming_detect_urg,incoming_etc,incoming_detect_etc,outgoing_syn,outgoing_detect_syn,outgoing_fin,outgoing_detect_fin,outgoing_rst,outgoing_detect_rst,outgoing_push,outgoing_detect_push,outgoing_ack,outgoing_detect_ack,outgoing_urg,outgoing_detect_urg,outgoing_etc,outgoing_detect_etc");
		add("ips_ddos_domain_learning",
				"time,machine_name,domain_id,all_pps,all_bps,tcp_pps,tcp_bps,udp_pps,udp_bps,icmp_pps,icmp_bps,etc_pps,etc_bps");
		// 4.1 - 4.23
		add("vpn_act_ike",
				"start_time,machine_name,tunnel_name,remote_gateway,key_exchange_step,key_exchange_result,priority,msg");
		add("vpn_act_ipsec", "start_time,machine_name,remote_gateway,direction,protocol,priority,spi,action,msg");
		add("vpn_act_event", "start_time,machine_name,msg");
		add("vpn_cnt_line_use",
				"time,machine_name,connection_ip,interface,tx_speed,rx_speed,tx_bytes,rx_bytes,tx_usages,rx_usages,fault_duration,connection_status");
		add("vpn_cnt_tunnel_use", "time,machine_name,tunnels,normal_tunnels,abnormal_tunnels");
		addOverlapped("vpn_cnt_traffic_remotegw",
				"time,machine_name,remote_gateway,encryption_packets,encryption_bytes,decryption_packets,decryption_bytes,usage");
		addOverlapped("vpn_cnt_traffic_remotegw",
				"time,machine_name,tunnel_id,encryption_packets,encryption_bytes,decryption_packets,decryption_bytes,usage");
		add("vpn_cnt_status_remotegw", "time,machine_name,connection_name,remote_gateway,src_interface,round_trip_time");
		add("vpn_cnt_speed_if", "time,machine_name,interface,src_ip,dst_ip,tx_speed,rx_speed");
		add("sslvpn3_act_access", "time,machine_name,server_ip,server_port,user_id,user_ip,assign_ip");
		add("sslvpn3_act_auth", "time,machine_name,user_id,user_ip,assign_ip,action,desc");
		add("sslvpn3_act_cert_issue", "time,machine_name,user_id,user_dn,certtime,status");
		add("sslvpn3_cnt_traffic", "time,machine_name,inbound_bytes,outbound_bytes,access_cnt");
		add("sslvpn3_cnt_server_access", "time,machine_name,server_ip,access_cnt");
		add("sslvpn3_cnt_login", "time,machine_name,login_cnt");
		add("sslvpn3_cnt_tunnel", "time,machine_name,web_tunnel_cnt,full_tunnel_cnt");
		add("sslvpn_act_access",
				"time,machine_name,user_id,src_ip,src_port,dst_ip,dst_port,access_url,request_uri,http_command,http_version,result");
		add("sslvpn_act_session",
				"start_time,end_time,machine_name,user_id,src_ip,src_port,protocol,bytes_forward,bytes_backward,terminate_reason");
		add("sslvpn_act_auth", "time,machine_name,session_id,user_id,src_ip,result");
		add("sslvpn_cnt_traffic", "time,machine_name,transactions,incoming_bytes,outgoing_bytes");
		add("sslvpn_cnt_service_traffic", "time,machine_name,service_name,transactions,incoming_bytes,outgoing_bytes");
		add("sslvpn_cnt_user_traffic", "time,machine_name,user_id,transactions,incoming_bytes,outgoing_bytes");
		add("sslvpn_cnt_concurrent", "time,machine_name,concurrents,request_counts");
		// 5.1 - 5.20
		add("app_act_antispam",
				"start_time,machine_name,fw_rule_id,src_ip,dst_ip,protocol,direction,sender_addr,receiver_addr,mail_bytes,mail_status,spam_type,action,msg");
		add("app_act_ftp", "start_time,machine_name,fw_rule_id,src_ip,dst_ip,inspect_type,action,ftp_command,msg");
		add("app_act_antivirus",
				"start_time,machine_name,fw_rule_id,src_ip,dst_ip,file_size,file_name,virus_name,protocol,direction,action,msg");
		add("app_act_webclient_all",
				"start_time,machine_name,fw_rule_id,client_ip,server_ip,tx_bytes,rx_bytes,http_command,transaction_id,result,msg");
		add("app_act_webclient_limit",
				"start_time,machine_name,fw_rule_id,client_ip,server_ip,tx_bytes,rx_bytes,http_command,transaction_id,result,msg");
		add("app_act_urlblock", "start_time,machine_name,fw_rule_id,src_ip,dst_ip,category,dst_name,uri");
		add("app_cnt_antispam",
				"start_time,machine_name,all_counts,allow_counts,detect_counts,alarm_counts,block_counts,protocol,direction");
		add("app_cnt_ftp", "start_time,machine_name,all_counts,upload_counts,download_counts,etc_counts,detect_counts");
		add("app_cnt_antivirus", "start_time,machine_name,all_counts,block_counts,detect_counts,protocol,direction");
		add("app_cnt_webclient_all", "start_time,machine_name,tx_bytes,rx_bytes");
		add("app_cnt_webclient_limit", "start_time,machine_name,content_limit_counts,abnormal_limit_counts");
		add("app_cnt_urlblock", "start_time,machine_name,block_counts");
		add("app_act_control_detect",
				"time,machine_name,src_ip,src_port,dst_ip,dst_port,fw_rule_id,profile_id,category_id,application_id,function_id,packet_dump_id,action,msg");
		add("app_cnt_control_papplication",
				"time,machine_name,application_id,detect_counts,block_counts,access_detect_counts,access_block_counts,login_detect_counts,login_block_counts,message_detect_counts,message_block_counts,file_detect_counts,file_block_counts,file_size_detect_counts,file_size_block_counts,audio_detect_counts,audio_block_counts,video_detect_counts,video_block_counts");
		add("app_cnt_control_pcategory",
				"time,machine_name,category_id,detect_counts,block_counts,access_detect_counts,access_block_counts,login_detect_counts,login_block_counts,message_detect_counts,message_block_counts,file_detect_counts,file_block_counts,file_size_detect_counts,file_size_block_counts,audio_detect_counts,audio_block_counts,video_detect_counts,video_block_counts");
		add("app_cnt_control_pprofile",
				"time,machine_name,profile_id,detect_counts,block_counts,access_detect_counts,access_block_counts,login_detect_counts,login_block_counts,message_detect_counts,message_block_counts,file_detect_counts,file_block_counts,file_size_detect_counts,file_size_block_counts,audio_detect_counts,audio_block_counts,video_detect_counts,video_block_counts");
		add("app_act_webserver_protect",
				"time,machine_name,fw_rule_id,profile_id,attack_id,attack_description,src_ip,src_port,dst_ip,dst_port,dst_url,packets,bytes,priority,action,mails,dump_id");
		add("app_cnt_webserver_protect", "time,machine_name,all_counts,all_bytes,detect_counts,detect_bytes");
		addOverlapped("app_act_officekeeper_list", "time,machine_name,action,block_ip,server,redirect_url");
		addOverlapped("app_act_officekeeper_list", "time,machine_name,src_ip,action,redirect_url");
	}

	public Mf2LogParser(Mode mode) {
		this.mode = mode;
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		try {
			String line = (String) params.get("line");
			if (line == null)
				return params;

			int b = line.indexOf('[');
			int e = line.indexOf(']', b);
			if (b < 0 || e < 0)
				return params;
			String type = line.substring(b + 1, e);

			b = line.indexOf('[', e + 1);
			e = line.indexOf(']', b + 1);
			if (b < 0 || e < 0)
				return params;
			String fromIp = line.substring(b + 1, e);

			Map<String, Object> m = new HashMap<String, Object>();
			m.put("log_type", type);
			m.put("from_ip", fromIp);

			int delimiter;
			if (mode == Mode.CSV) {
				delimiter = ',';
			} else {
				delimiter = '\t';
			}

			String[] fields = typeFieldMap.get(type);
			if (fields == null) {
				List<String[]> fieldsList = overlappedFieldMap.get(type);

				if (fieldsList == null) {
					if (slog.isDebugEnabled())
						slog.debug("krsyslog parser: unknown mf2 log type [{}], line [{}]", type, line);
					return params;
				} else {
					String newLine = line.substring(e + 2);

					String splitStr = (delimiter == ',') ? "," : "\t";
					int numOfFields = newLine.split(splitStr).length;

					for (String[] s : fieldsList) {
						if (s.length == numOfFields) {
							parse(line, m, s, e, delimiter);
							return m;
						}
					}
					return params;
				}
			}

			parse(line, m, fields, e, delimiter);
			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled()) {
				String line = (String) params.get("line");
				slog.debug("araqne krsyslog parser: cannot parse log [" + line + "]", t);
			}

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
		}
	}
}