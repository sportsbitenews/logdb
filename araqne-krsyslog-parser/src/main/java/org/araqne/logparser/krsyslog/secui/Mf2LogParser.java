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
		// set system logs
		add("audit", "time,machid,adminid,adminip,level,menuid,paramstr,resultcode,reason");
		add("alert", "alert_level,time,category,machid,desc");
		add("ha_event", "time,machid,evtype,evstr");
		add("ha_traffic", "time,machid,opkts,ipkts,obytes,ibytes,traffictype");
		add("ha_status", "time,members,machid,action,status");
		add("mng_iap_interworking", "time,machid,sensorip,attackerip,attackerport,victimip,victimport,protocol,blocktime,msg");
		add("mng_zombie_pc_solution", "time,machid,solution_addr,infected_bot_addr,attacker_addr,block_period");
		add("mng_blacklist", "time,machid,srcip,srcport,dstip,dstport,protocol,blocktime,pkt,byte,type,reason");
		add("mng_blacklist_ipv6", "time,machid,srcip,srcport,dstip,dstport,protocol,blocktime,pkt,byte,type,reason");
		add("mng_line", "time,machid,ifname,descstr");
		add("mng_resource",
				"time,machid,cpu_temp(℃),cpu_usage(%),memory_capacity(KB),memory_usage(%),disk_capacity(KB),disk_usage(%),cpu0_usage(%),cpu1_usage(%)");
		add("mng_daemon", "time,machid,daemon_name,cpu_use,vmuse,rmuse");
		add("mng_if_traffic", "timebuf,machid,daemon_name,status,rxfrms,txfrms,rxbytes,txbytes");
		add("mng_oversubscription",
				"time,machid,inbound_bypass_packet,inbound_bypass_byte,inbound_drop_packet,inbound_drop_byte,outbound_bypass_packet,outbound_bypass_byte,outbound_drop_packet,outbound_drop_byte");
		add("mng_qos", "time,machid,queue_name,using_bw,using_rate,loss_bw,loss_rate");
		add("mng_fqdn_object_management", "time,machid,objectname,type,before,after");
		add("mng_user_object_management", "time,machid,objectname,type,before,after");

		// set firewall logs
		add("fw4_allow",
				"stime,etime,dtime,machid,ruleid,natruledid,srcip,srcport,dstip,dstport,proto,zone,ipkts,opkts,ibytes,obytes,fragment,flag,result");
		add("fw4_deny",
				"stime,etime,dtime,machid,ruleid,natruleid,srcip,srcport,dstip,dstport,proto,zone,pkts,bytes,fragment,flag,result");
		add("fw6_allow",
				"stime,etime,dtime,machid,ruleid,srcip6,srcport,dstip6,dstport,proto,zone,ipkts,opkts,inbytes,obytes,exthdr,fragment,flag,result");
		add("fw6_deny",
				"stime,etime,dtime,machid,ruleid,srcip6,srcport,dstip6,dstport,proto,zone,pkts,bytes,exthdr,fragment,flag,result");
		add("nat_session",
				"stime,etime,machid,nat_policy_id,firewall_policy_id,interface,src_ip,src_port,dst_ip,dst_port,protocol,converted_src_ip,converted_src_port,converted_dst_ip,converted_dst_port,send_packets,recv_packets,send_bytes,recv_bytes");
		add("fw4_traffic", "time,machid,admitpkts,denypkts,acc_sesscnt,peak_sesscnt,admitbytes,denybytes");
		add("fw4_rule_traffic", "time,machid,ruleid,action,pkts,bytes,acc_sessions,peak_sessions");
		add("fw6_traffic", "time,machid,admitpkts,denypkts,acc_sesscnt,peak_sesscnt,admitbytes,denybytes");
		add("fw6_rule_traffic", "time,machid,ruleid,action,pkts,bytes,acc_sessions,peak_sessions");
		add("nat_traffic", "time,machid,pkts,bytes,acc_sessions,peak_sessions");
		add("nat_rule_traffic", "time,machid,natruleid,pkts,bytes,acc_sessions,peak_sessions");
		add("fw_act_userauth", "time,machid,userid,authserver,ip,action,result,details");
		add("fw_cnt_usertraffic", "time,machid,userid,rxpackets,txpackets,rxbytes,txbytes,sessions,acc_sessions");

		// set ips/ddos logs
		add("ips_ddos_detect",
				"deviceid,start,end,domain,attacktype,attackid,attackerip,victimip,attackerport,victimport,pktt,dump,byte,eventid,tcpflag,ipflag,priority,protocol,act,attackermac");
		add("ips_ddos_incident", "deviceid,start,end,packet,byte,eventid,attackid,domain,priority,act,sendmail,status");
		add("ips_ddos_traffic",
				"time,machid,inbound_pps / 10.0,inbound_detect_pps / 10.0,inbound_bps / 10.0,inbound_detect_bps / 10.0,outbound_pps / 10.0,outbound_detect_pps / 10.0,outbound_bps / 10.0,outbound_detect_bps / 10.0");
		add("ips_ddos_domain_traffic_proto",
				"time,machid,domain_id,inbound_tcp,inbound_detect_tcp,inbound_udp,inbound_detect_udp,inbound_icmp,inbound_detect_icmp,inbound_other,inbound_detect_other,outbound_tcp,outbound_detect_tcp,outbound_udp,outbound_detect_udp,outbound_icmp,outbound_detect_icmp,outbound_other,outbound_detect_other");
		add("ips_ddos_domain_traffic_protofrag",
				"time,machid,domain,incoming_tcp,incoming_tcp_detect,incoming_udp,incoming_udp_detect,incoming_icmp,incoming_icmp_detect,incoming_etc,incoming_etc_detect,outgoing_tcp,outgoing_tcp_detect,outgoing_udp,outgoing_udp_detect,outgoing_icmp,outgoing_icmp_detect,outgoing_etc,outgoing_etc_detect");
		add("ips_ddos_domain_traffic_payload",
				"time,machid,domain,incoming_64,incoming_64_detect,incoming_128,incoming_128_detect,incoming_256,incoming_256_detect,incoming_512,incoming_512_detect,incoming_1024,incoming_1024_detect,incoming_1518,incoming_1518_detect,outgoing_64,outgoing_64_detect,outgoing_128,outgoing_128_detect,outgoing_256,outgoing_256_detect,outgoing_512,outgoing_512_detect,outgoing_1024,outgoing_1024_detect,outgoing_1518,outgoing_1518_detect");
		add("ips_ddos_domain_traffic_ttl",
				"time,machid,domain,incoming_64,incoming_64_detect,incoming_128,incoming_128_detect,incoming_160,incoming_160_detect,incoming_192,incoming_192_detect,incoming_234,incoming_234_detect,incoming_256,incoming_256_detect,outgoing_64,outgoing_64_detect,outgoing_128,outgoing_128_detect,outgoing_160,outgoing_160_detect,outgoing_192,outgoing_192_detect,outgoing_234,outgoing_234_detect,outgoing_256,outgoing_256_detect");
		add("ips_ddos_domain_traffic_tcpflag",
				"time,machid,domain,incoming_syn,incoming_syn_detect,incoming_fin,incoming_fin_detect,incoming_rst,incoming_rst_detect,incoming_push,incoming_push_detect,incoming_ack,incoming_ack_detect,incoming_urg,incoming_urg_detect,incoming_etc,incoming_etc_detect,outgoing_syn,outgoing_syn_detect,outgoing_fin,outgoing_fin_detect,outgoing_rst,outgoing_rst_detect,outgoing_push,outgoing_push_detect,outgoing_ack,outgoing_ack_detect,outgoing_urg,outgoing_urg_detect,outgoing_etc,outgoing_etc_detect");
		add("ips_ddos_domain_learning",
				"time,machid,domain_id,total_pps,total_bps,tcp_pps,tcp_bps,udp_pps,udp_bps,icmp_pps,icmp_bps,etc_pps,etc_bps");

		// set vpn logs
		add("vpn_act_ike", "time,machid,tunnelname,remotegateway,step,result,priority,desc");
		add("vpn_act_ipsec", "time,utime,machid,remotegw,conndir,srcflow,dstflow,protocol,priority,spi,action,desc");
		add("vpn_act_event", "time,machid,desc");
		add("vpn_cnt_line_use",
				"time,machid,circuitip,ifname,txspeed,rxspeed,txbytes,rxbytes,txusage,rxusage,failureperiod,status");
		add("vpn_cnt_tunnel_use", "time,machid,totaltunnel,normaltunnel,abnormaltunnel");
		add("vpn_cnt_traffic_remotegw", "time,machid,remotegw,encpackets,encbytes,decpackets,decbytes");
		add("vpn_cnt_traffic_tunnelid", "time,machid,tunnelid,encpackets,encbytes,decpackets,decbytes");
		add("vpn_cnt_status_remotegw", "time,machid,conn_name,remotegwip,ifname,rtt");
		add("vpn_cnt_speed_if", "time,machid,iface,srcip,dstip,tx_speed,rx_speed");

		// set ssl vpn logs
		add("sslvpn3_act_access", "time,machid,srvip,srvport,userid,userip,asgip");
		add("sslvpn3_act_auth", "time,machid,userid,userip,asgip,action,descstr");
		add("sslvpn3_act_cert_issue", "time,machid,userid,dnstr,certstime,certetime,type");
		add("sslvpn3_cnt_traffic", "time,machid,inbytes,outbytes,accesscnt");
		add("sslvpn3_cnt_server_access", "time,machid,srvip,accesscnt");
		add("sslvpn3_cnt_login", "time,machid,logincnt");
		add("sslvpn3_cnt_tunnel", "time,machid,webaccesscnt,fullaccesscnt");
		add("sslvpn_act_access", "time,machid,userid,srcip,srcport,dstip,dstport,urlstr,uristr,method,version,result");
		add("sslvpn_act_session", "stime,etime,machid,userid,srcip,srcport,protocol,forward_bytes,backward_bytes,result");
		add("sslvpn_act_auth", "time,machid,sessionid,userid,srcip,result");
		add("sslvpn_cnt_traffic", "time,machid,transaction,in_bytes,out_bytes");
		add("sslvpn_cnt_service_traffic", "time,machid,service_name,transaction,in_bytes,out_bytes");
		add("sslvpn_cnt_user_traffic", "time,machid,userid,transaction,in_bytes,out_bytes");
		add("sslvpn_cnt_concurrent", "time,machid,concurrent_count,req_count");

		// set application security logs
		add("app_act_ssl4_session",
				"start_time,end_time,duration,machid,ssl_id,client_ip,client_port,server_ip,server_port,protocol,auth_info,status,bytes");
		add("app_cnt_ssl4_traffic",
				"time,machid,protocol,dec_sessions,dec_bytes,except_sessions,except_bytes,error_session,error_bytes");
		add("app_act_ssl4_event", "time,machid,ssl_id,client_ip,client_port,protocol,status,message");
		add("app_act_ssl6_session",
				"start_time,end_time,duration,machid,ssl_id,client_ip,client_port,server_ip,server_port,protocol,auth_info,status,bytes");
		add("app_cnt_ssl6_traffic",
				"time,machid,protocol,dec_sessions,dec_bytes,except_sessions,except_bytes,error_session,error_bytes");
		add("app_act_ssl6_event", "time,machid,ssl_id,client_ip,client_port,protocol,status,message");
		add("app_act_control_detect",
				"machid,time,srcip,dstip,srcport,dstport,fwruleid,moduleid,eventid,profileid,categoryid,appid,funcid,transprotocolid,pdumpid,email,protocol,action,msglen,msg");
		add("app_cnt_control_papplication",
				"time,machid,appid,detectcnt,blockcntmsgdetectcnt,msgblockcnt,filedetectcnt,fileblockcnt,filesizedetectcnt,filesizeblockcnt");
		add("app_cnt_control_pcategory",
				"time,machid,categoryid,detectcnt,,blockcntmsgdetectcnt,msgblockcnt,filedetectcnt,fileblockcnt,filesizedetectcnt,filesizeblockcnt");
		add("app_cnt_control_pprofile",
				"time,machid,profileid,detectcnt,,blockcntmsgdetectcnt,msgblockcnt,filedetectcnt,fileblockcnt,filesizedetectcnt,filesizeblockcnt");
		add("app_act_dlp_detect",
				"time,machid,src_ip,src_port,dst_ip,dst_port,protocol,direction,firewall_id,profile_id,object_name,target,value,host,sender,dump_id,action,message");
		add("app_act_dlp_archive",
				"time,machid,src_ip,src_port,dst_ip,dst_port,protocol,direction,firewall_id,profile_id,object_name,target,archive_size,archive_file_name,archive_file_id");
		add("app_cnt_dlp_stats", "time,machid,protocol,direction,total_count,detect_count,block_count,archive_count");
		add("app_act_antivirus",
				"time,machid,ruleid,srcip,dstip,filelen,filenamelen,virusnamelen,desclen,counteract,protocol,direction,filename,virusname,desc");
		add("app_cnt_antivirus", "time,machid,totalcnt,blockcnt,detectcnt,protocol,direction");
		add("app_act_antispam",
				"time,machid,ruleid,srcip,dstip,transaction,mailsz,senderlen,receiverlen,desclen,protocol,maildif,reacttype,status,spamtype,sendaddr,rcvaddr,desc");
		add("app_cnt_antispam", "time,machid,totalcnt,allowcnt,detectcnt,noticecnt,blockcnt,protocol,direction");
		add("app_act_urlblock", "time,machid,ruleid,srcip,dstip,categorylen,destlen,urilen,category,dstname,uri");
		add("app_cnt_urlblock", "time,machid,blockcnt");
		add("app_act_cloud_url_block", "time,machid,ssl_id,source_ip,destination_ip,host,url");
		add("app_cnt_cloud_url_block", "time,machid,total_url_cnt,allowed_url_cnt,blocked_url_cnt");
		add("app_cnt_categorized_url_detect", "time,machid,large_category,small_category,detect_count");
		add("app_cnt_categorized_url_block", "time,machid,large_category,small_category,block_count");
		add("app_act_webclient_all",
				"time,machid,ruleid,clientip,serverip,toserver,toclient,transactionid,method,result,desclen,desc");
		add("app_act_webclient_limit",
				"time,machid,ruleid,clientip,serverip,toserver,toclient,transactionid,method,result,desclen,desc");
		add("app_cnt_webclient_all", "time,machid,toserver,toclient");
		add("app_cnt_webclient_limit", "time,machid,toserver,toclient");
		add("app_act_webserver_protect",
				"time,machid,ruleid,profileid,attackid,attackdesc_str,srcip,srcport,dstip,dstport,desturl_str,pkts,bytes,priority,counteract,sendmail,dumped");
		add("app_cnt_webserver_protect", "time,machid,totalcnt,totalbytes,detectcnt,detectbytes");
		add("app_act_officekeeper_list", "time,machid,mode,blockip,server,redirecturl");
		add("app_act_officekeeper_block", "time,machid,srcip,mode,redirecturl");
		add("app_act_ftp", "time,machid,ruleid,srcip,dstip,inspecttype,counteract,desclen,ftpcmd,desc");
		add("app_cnt_ftp", "time,machid,totalcnt,upcnt,downcnt,etccnt,detectcnt");
		add("app_act_apt_malware",
				"time,machid,rule_id,profile_id,sender or source_ip,recipient or destination_ip,gather_path,name_type,url_file,detect_name,analysis_request,analysis_result,cnc_server,action,ticket_id");
		add("app_cnt_apt_malware", "time,machid,rule_id,profile_id,source_ip,destination_ip,occur_list,action");
		add("app_act_apt_heuristic", "time,machid,safety_count,warning_count,danger_count,fail_count");

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