/*
 * Copyright 2012 Future Systems
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
package org.araqne.logparser.krsyslog.ahnlab;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.log.api.FieldDefinition;
import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrusGuardLogParser extends V1LogParser {
	private final Logger logger = LoggerFactory.getLogger(TrusGuardLogParser.class.getName());

	private static final List<FieldDefinition> fields;

	static {
		fields = new ArrayList<FieldDefinition>();
		addField("vlan_id", "string");
		addField("reason", "string");
		addField("src_port", "string");
		addField("allow_pps", "string");
		addField("risk_score_db", "string");
		addField("tx_nif", "string");
		addField("event", "string");
		addField("threshold_byte", "string");
		addField("sender_addr", "string");
		addField("tcp_flag", "string");
		addField("connection_coun", "string");
		addField("request_count", "string");
		addField("accuracy_score_db", "string");
		addField("user_type", "string");
		addField("allow_etc_pps", "string");
		addField("pps", "string");
		addField("accuracy_action_user", "string");
		addField("policy_id", "string");
		addField("in_pkt", "string");
		addField("deny_bps", "string");
		addField("ha", "string");
		addField("qos_name", "string");
		addField("drop_packets", "string");
		addField("session", "string");
		addField("send_spam_log", "string");
		addField("risk_action_user", "string");
		addField("src_ip", "string");
		addField("block_session", "string");
		addField("log_type", "string");
		addField("out_zone", "string");
		addField("rule_id", "string");
		addField("nat_ip", "string");
		addField("desc", "string");
		addField("subject", "string");
		addField("rx_nif", "string");
		addField("mac", "string");
		addField("snat_port", "string");
		addField("date", "string");
		addField("spam_filter", "string");
		addField("allow_bps", "string");
		addField("version", "string");
		addField("in_data", "string");
		addField("drop_udp_bps", "string");
		addField("utm_id", "string");
		addField("risk_score_user", "string");
		addField("src", "string");
		addField("log_id", "string");
		addField("group_name", "string");
		addField("pkt_len", "string");
		addField("log_sub_type", "string");
		addField("inst_code", "string");
		addField("profile_name", "string");
		addField("sent_data", "string");
		addField("deny_pps", "string");
		addField("drop_etc_pps", "string");
		addField("diffusion_score_db", "string");
		addField("dnat_port", "string");
		addField("drop_tcp_bps", "string");
		addField("network_direction", "string");
		addField("network_id", "string");
		addField("nif_name", "string");
		addField("eth_name", "string");
		addField("allow_bytes", "string");
		addField("sent_pkt", "string");
		addField("drop_bytes", "string");
		addField("count", "string");
		addField("dnat_ip", "string");
		addField("alarm_type", "string");
		addField("dst_ip", "string");
		addField("ap_protocol", "string");
		addField("eth_protocol", "string");
		addField("virus_name", "string");
		addField("mask", "string");
		addField("attack_rate", "string");
		addField("dst", "string");
		addField("type", "string");
		addField("accuracy_score_user", "string");
		addField("filter_status", "string");
		addField("drop_icmp_bps", "string");
		addField("allow_tcp_pps", "string");
		addField("allow_icmp_pps", "string");
		addField("encrypt", "string");
		addField("description", "string");
		addField("action", "string");
		addField("rcvd_data", "string");
		addField("nat_port", "string");
		addField("allow_tcp_bps", "string");
		addField("nat_id", "string");
		addField("user_addr", "string");
		addField("status", "string");
		addField("cpu", "string");
		addField("wf_type", "string");
		addField("rcvd_pkt", "string");
		addField("allow_session", "string");
		addField("code", "string");
		addField("expire_time", "string");
		addField("slice_seconds", "string");
		addField("out_pkt", "string");
		addField("msg", "string");
		addField("url", "string");
		addField("epsec", "string");
		addField("snat_type", "string");
		addField("nif_type", "string");
		addField("threshold_packets", "string");
		addField("drop_udp_pps", "string");
		addField("allow_udp_pps", "string");
		addField("recipients_addr", "string");
		addField("drop_tcp_pps", "string");
		addField("module_flag", "string");
		addField("module_name", "string");
		addField("in_zone", "string");
		addField("virus_url", "string");
		addField("out_data", "string");
		addField("dst_port", "string");
		addField("state", "string");
		addField("in_nic", "string");
		addField("nat_type", "string");
		addField("virus_filter", "string");
		addField("snat_ip", "string");
		addField("out_nic", "string");
		addField("app_name", "string");
		addField("allow_icmp_bps", "string");
		addField("rate_limit", "string");
		addField("zone_name", "string");
		addField("bps", "string");
		addField("mem", "string");
		addField("drop_etc_bps", "string");
		addField("allow_udp_bps", "string");
		addField("virus_fname", "string");
		addField("allow_etc_bps", "string");
		addField("protocol", "string");
		addField("nif", "string");
		addField("logtype", "string");
		addField("src_mac", "string");
		addField("severity", "string");
		addField("diag_name", "string");
		addField("ip", "string");
		addField("message", "string");
		addField("ip_ver", "string");
		addField("diffusion_action_user", "string");
		addField("dnat_type", "string");
		addField("duration", "string");
		addField("diffusion_score_user", "string");
		addField("hdd", "string");
		addField("attack_id", "string");
		addField("drop_icmp_pps", "string");
		addField("user", "string");
		addField("allow_packets", "string");
		addField("botnet_name", "string");
		addField("alert_id", "string");
	}

	private static void addField(String name, String type) {
		fields.add(new FieldDefinition(name, type));
	}

	@Override
	public List<FieldDefinition> getFieldDefinitions() {
		return fields;
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		String line = (String) params.get("line");
		if (line == null)
			return null;

		String[] tokenizedLine = tokenizeLine(line, "`");
		try {
			Map<String, Object> m = new HashMap<String, Object>();
			int index = 0;
			String versionString = tokenizedLine[index++];
			if (versionString.equals("DPX1")) {
				m.put("version", versionString);
			} else {
				int version = Integer.parseInt(versionString);
				m.put("version", version);
			}
			m.put("encrypt", Integer.valueOf(tokenizedLine[index++]));

			int type = Integer.valueOf(tokenizedLine[index++]);
			m.put("type", type);
			m.put("count", Integer.valueOf(tokenizedLine[index++]));
			m.put("utm_id", tokenizedLine[index++]);

			if (versionString.equals("1")) {
				if (type == 1) { // kernel log (packet filter)
					parseFirewallLogV1(tokenizedLine, m);
				} else if (type == 2) { // application log
					parseApplicationLogV1(tokenizedLine, m);
				}
			} else if (versionString.equals("3") || versionString.equals("DPX1")) {
				Integer moduleFlag = Integer.valueOf(tokenizedLine[index++]);
				m.put("module_flag", moduleFlag);
				// log data
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
				String dateToken = tokenizedLine[index++];
				String timeToken = tokenizedLine[index++];
				try {
					m.put("date", dateFormat.parse(dateToken + " " + timeToken));
				} catch (ParseException e) {
				}

				if (type == 1) { // kernel log (packet filter)
					parseFirewallLogV3(tokenizedLine, m);
				} else if (type == 2) { // application log
					parseApplicationLogV3(tokenizedLine, moduleFlag, m);
				}
			}

			return m;
		} catch (Throwable t) {
			logger.debug("araqne syslog parser: cannot parse trusguard log => " + line, t);
			return null;
		}
	}

	private void parseApplicationLogV3(String[] tokenizedLine, Integer moduleFlag, Map<String, Object> m) {
		if (moduleFlag == 1010) {// operation
			parseOperationLogV3(tokenizedLine, m);
		} else if (moduleFlag == 1011) {// stat
			parseStatLogV3(tokenizedLine, m);
		} else if (moduleFlag == 1050) {// webfilter
			parseWebFilterLogV3(tokenizedLine, m);
		} else if (moduleFlag == 1070) {// app filter
			parseAppFilterLogV3(tokenizedLine, m);
		} else if (moduleFlag == 1100) {// IPS
			parseIpsLogV3(tokenizedLine, m);
		} else if (moduleFlag == 1110) {// DNS
			parseDnsLogV3(tokenizedLine, m);
		} else if (moduleFlag == 1120 || moduleFlag == 1121) {// IAC
			parseInternetAccessControlLogV3(tokenizedLine, moduleFlag, m);
		} else if (moduleFlag == 1140) {// Qos
			parseQosLogV3(tokenizedLine, m);
		} else if (moduleFlag == 1141) {// LBQos
			parseLbqosLogV3(tokenizedLine, m);
		} else if (moduleFlag == 1150 || moduleFlag == 1151) {// proxy
			parseProxyLogV3(tokenizedLine, m);
		} else if (moduleFlag == 1160) {// system quarantine
			parseSystemQuarantineLogV3(tokenizedLine, m);
		} else if (moduleFlag == 3010 || moduleFlag == 1) {
			parseManagementOperaionLogV3(tokenizedLine, m);
		} else if (moduleFlag == 3011) {
			parseManagementStatLogV3(tokenizedLine, m);
		} else if (moduleFlag == 100) {
			parseManagementStatLogV2(tokenizedLine, m);
		} else if (moduleFlag == 3012) {
			parseManagementNetworkPortLogV3(tokenizedLine, m);
		} else if (moduleFlag == 3020 || moduleFlag == 9) {
			parseDpxIpsLogV3(tokenizedLine, m);
		} else if (moduleFlag == 3030 || moduleFlag == 3080 || moduleFlag == 3120 || moduleFlag == 3150 || moduleFlag == 101
				|| moduleFlag == 107 || moduleFlag == 108 || moduleFlag == 109 || moduleFlag == 110 || moduleFlag == 111
				|| moduleFlag == 113 || moduleFlag == 114) {
			parseUntrustedTrafficBlockFilterStatusLogV3(tokenizedLine, m);
		} else if (moduleFlag == 3031 || moduleFlag == 3041 || moduleFlag == 3051 || moduleFlag == 3061 || moduleFlag == 3071
				|| moduleFlag == 3121 || moduleFlag == 3131 || moduleFlag == 3141 || moduleFlag == 3151 || moduleFlag == 3191
				|| moduleFlag == 3211 || moduleFlag == 3181 || moduleFlag == 3201) {
			parseUntrustedTrafficBlockFilterBlockLogV3(tokenizedLine, m);
		} else if (moduleFlag == 3040 || moduleFlag == 3090 || moduleFlag == 3100 || moduleFlag == 3110 || moduleFlag == 3130
				|| moduleFlag == 3140 || moduleFlag == 103 || moduleFlag == 112) {
			parseNetworkProtectionbySegmentStatusLogV3(tokenizedLine, m);
		} else if (moduleFlag == 3050 || moduleFlag == 3060 || moduleFlag == 3210 || moduleFlag == 104 || moduleFlag == 105) {
			parseAntiSpoofingProtectionStatusLogV3(tokenizedLine, m);
		} else if (moduleFlag == 3052 || moduleFlag == 3212 || moduleFlag == 3062) {
			parseAntiSpoofingProtectionAuthLogV3(tokenizedLine, m);
		} else if (moduleFlag == 3070 || moduleFlag == 106) {
			parseStatefulPacketInspectionStatusLogV3(tokenizedLine, m);
		} else if (moduleFlag == 3160 || moduleFlag == 201) {
			parseSegmentProtectionFilterStatusLogV3(tokenizedLine, m);
		} else if (moduleFlag == 3170 || moduleFlag == 3171 || moduleFlag == 210 || moduleFlag == 211) {
			parseAttackLogV3(tokenizedLine, m);
		} else if (moduleFlag == 6010) {
			parseManagementSystemLogV3(tokenizedLine, m);
		} else if (moduleFlag == 6011) {
			parseManagementSystemStatusLogV3(tokenizedLine, m);
		} else if (moduleFlag == 6030) {
			parseIpxIpsLogV3(tokenizedLine, m);
		} else if (moduleFlag == 6031) {
			parseIpxApplicationControlLogV3(tokenizedLine, m);
		} else if (moduleFlag == 6040) {
			parseIpxQosLogV3(tokenizedLine, m);
		} else if (moduleFlag == 6050) {
			parseSystemIsolationLogV3(tokenizedLine, m);
		} else if (moduleFlag == 6060) {
			parseCncDetectionLogV3(tokenizedLine, m);
		}
	}

	private void parseCncDetectionLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module flag 6060
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));
		m.put("module_name", tokenizedLine[index++]);
		m.put("profile_name", tokenizedLine[index++]);

		String riskScoreDb = tokenizedLine[index++];
		if (riskScoreDb.equals("1")) {
			riskScoreDb = "낮음";
		} else if (riskScoreDb.equals("2")) {
			riskScoreDb = "보통";
		} else if (riskScoreDb.equals("3")) {
			riskScoreDb = "높음";
		}
		m.put("risk_score_db", riskScoreDb);

		String riskScoreUser = tokenizedLine[index++];
		if (riskScoreUser.equals("0")) {
			riskScoreUser = "사용안함";
		} else if (riskScoreUser.equals("1")) {
			riskScoreUser = "낮음";
		} else if (riskScoreUser.equals("2")) {
			riskScoreUser = "보통";
		} else if (riskScoreUser.equals("3")) {
			riskScoreUser = "높음";
		}
		m.put("risk_score_user", riskScoreUser);

		String riskActionUser = tokenizedLine[index++];
		if (riskActionUser.equals("0")) {
			riskActionUser = "허용";
		} else if (riskActionUser.equals("1")) {
			riskActionUser = "차단";
		}
		m.put("risk_action_user", riskActionUser);

		String diffusionScoreDb = tokenizedLine[index++];
		if (diffusionScoreDb.equals("1")) {
			diffusionScoreDb = "낮음";
		} else if (diffusionScoreDb.equals("2")) {
			diffusionScoreDb = "보통";
		} else if (diffusionScoreDb.equals("3")) {
			diffusionScoreDb = "높음";
		}
		m.put("diffusion_score_db", diffusionScoreDb);

		String diffusionScoreUser = tokenizedLine[index++];
		if (diffusionScoreUser.equals("0")) {
			diffusionScoreUser = "사용안함";
		} else if (diffusionScoreUser.equals("1")) {
			diffusionScoreUser = "낮음";
		} else if (diffusionScoreUser.equals("2")) {
			diffusionScoreUser = "보통";
		} else if (diffusionScoreUser.equals("3")) {
			diffusionScoreUser = "높음";
		}
		m.put("diffusion_score_user", diffusionScoreUser);

		String diffusionActionUser = tokenizedLine[index++];
		if (diffusionActionUser.equals("0")) {
			diffusionActionUser = "허용";
		} else if (diffusionActionUser.equals("1")) {
			diffusionActionUser = "차단";
		}
		m.put("diffusion_action_user", diffusionActionUser);

		String accuracyScoreDb = tokenizedLine[index++];
		if (accuracyScoreDb.equals("1")) {
			accuracyScoreDb = "낮음";
		} else if (accuracyScoreDb.equals("2")) {
			accuracyScoreDb = "보통";
		} else if (accuracyScoreDb.equals("3")) {
			accuracyScoreDb = "높음";
		}
		m.put("accuracy_score_db", accuracyScoreDb);

		String accuracyScoreUser = tokenizedLine[index++];
		if (accuracyScoreUser.equals("0")) {
			accuracyScoreUser = "사용안함";
		} else if (accuracyScoreUser.equals("1")) {
			accuracyScoreUser = "낮음";
		} else if (accuracyScoreUser.equals("2")) {
			accuracyScoreUser = "보통";
		} else if (accuracyScoreUser.equals("3")) {
			accuracyScoreUser = "높음";
		}
		m.put("accuracy_score_user", accuracyScoreUser);

		String accuracyActionUser = tokenizedLine[index++];
		if (accuracyActionUser.equals("0")) {
			accuracyActionUser = "허용";
		} else if (accuracyActionUser.equals("1")) {
			accuracyActionUser = "차단";
		}
		m.put("accuracy_action_user", accuracyActionUser);

		String botnetName = tokenizedLine[index++];
		m.put("botnet_name", botnetName.isEmpty() ? null : botnetName);
		String diagName = tokenizedLine[index++];
		m.put("diag_name", diagName.isEmpty() ? null : diagName);
		String instCode = tokenizedLine[index++];
		m.put("inst_code", instCode.isEmpty() ? null : instCode);
	}

	private void parseSystemIsolationLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module flag 6050
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));

		String action = tokenizedLine[index++];
		if (action.equals("0"))
			action = "격리";
		if (action.equals("1"))
			action = "웹리디렉션";
		if (action.equals("2"))
			action = "세션차단";
		m.put("action", action);
		String userToken = tokenizedLine[index++];
		m.put("user", userToken.isEmpty() ? null : userToken);
		m.put("module_name", tokenizedLine[index++]);
		String desc = tokenizedLine[index++];
		m.put("desc", desc.isEmpty() ? null : desc);
		String instCode = tokenizedLine[index++];
		m.put("inst_code", instCode.isEmpty() ? null : instCode);
	}

	private void parseIpxQosLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module flag 6040
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));
		m.put("action", tokenizedLine[index++]);
		String userToken = tokenizedLine[index++];
		m.put("user", userToken.isEmpty() ? null : userToken);
		m.put("module_name", tokenizedLine[index++]);
		m.put("qos_name", tokenizedLine[index++]);
		m.put("eth_name", tokenizedLine[index++]);
		m.put("bps", Long.parseLong(tokenizedLine[index++]));
		m.put("pps", Long.parseLong(tokenizedLine[index++]));
		String instCode = tokenizedLine[index++];
		m.put("inst_code", instCode.isEmpty() ? null : instCode);
	}

	private void parseIpxApplicationControlLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module flag 6031
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));

		String action = tokenizedLine[index++];
		if (action.equals("3001"))
			action = "차단";
		else if (action.equals("3003"))
			action = "허용";
		else if (action.equals("3006"))
			action = "세션 끊기";
		m.put("action", action);

		String userToken = tokenizedLine[index++];
		m.put("user", userToken.isEmpty() ? null : userToken);
		m.put("module_name", tokenizedLine[index++]);
		m.put("profile_name", tokenizedLine[index++]);
		m.put("group_name", tokenizedLine[index++]);
		m.put("app_name", tokenizedLine[index++]);

		String alarmType = tokenizedLine[index++];
		m.put("alarm_type", alarmType.isEmpty() ? null : alarmType);
		String desc = tokenizedLine[index++];
		m.put("desc", desc.isEmpty() ? null : desc);
		m.put("rule_id", Long.parseLong(tokenizedLine[index++]));
		m.put("log_id", Long.parseLong(tokenizedLine[index++]));
		String instCode = tokenizedLine[index++];
		m.put("inst_code", instCode.isEmpty() ? null : instCode);
	}

	private void parseIpxIpsLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module flag 6030
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));

		String action = tokenizedLine[index++];
		if (action.equals("3001"))
			action = "차단";
		else if (action.equals("3002"))
			action = "시스템 격리";
		else if (action.equals("3003"))
			action = "허용";
		else if (action.equals("3006"))
			action = "세션 끊기";
		else if (action.equals("3007"))
			action = "출발지 IP기준 사용량 제한";
		else if (action.equals("3008"))
			action = "목적지 IP기준 사용량 제한";
		else if (action.equals("3009"))
			action = "DDos 공격 차단";
		else if (action.equals("3010"))
			action = "일괄차단";
		m.put("action", action);

		String userToken = tokenizedLine[index++];
		m.put("user", userToken.isEmpty() ? null : userToken);
		m.put("module_name", tokenizedLine[index++]);
		m.put("reason", tokenizedLine[index++]);
		m.put("rx_nif", tokenizedLine[index++]);
		m.put("eth_protocol", tokenizedLine[index++]);
		m.put("src_mac", tokenizedLine[index++]);
		m.put("rule_id", Long.parseLong(tokenizedLine[index++]));
		m.put("vlan_id", tokenizedLine[index++]);
		m.put("message", tokenizedLine[index++]);
		String instCode = tokenizedLine[index++];
		m.put("inst_code", instCode.isEmpty() ? null : instCode);
		m.put("tx_nif", tokenizedLine[index++]);

		m.put("network_direction", tokenizedLine[index++]);
		m.put("network_id", tokenizedLine[index++]);
		m.put("log_id", Long.parseLong(tokenizedLine[index++]));
		m.put("profile_name", tokenizedLine[index++]);
		m.put("group_name", tokenizedLine[index++]);

	}

	private void parseManagementSystemStatusLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module flag 6011
		int index = 8;
		m.put("module_name", tokenizedLine[index++]);
		m.put("cpu", Integer.valueOf(tokenizedLine[index++]));
		m.put("mem", Integer.valueOf(tokenizedLine[index++]));
		m.put("hdd", Integer.valueOf(tokenizedLine[index++]));
		m.put("session", Integer.valueOf(tokenizedLine[index++]));
		m.put("in_data", Long.parseLong(tokenizedLine[index++]));
		m.put("out_data", Long.parseLong(tokenizedLine[index++]));
		m.put("in_pkt", Long.parseLong(tokenizedLine[index++]));
		m.put("out_pkt", Long.parseLong(tokenizedLine[index++]));
		m.put("ha", tokenizedLine[index++]);

		String instCode = tokenizedLine[index++];
		m.put("inst_code", instCode.isEmpty() ? null : instCode);
		m.put("allow_pps", Long.parseLong(tokenizedLine[index++]));
		m.put("deny_pps", Long.parseLong(tokenizedLine[index++]));
		m.put("allow_bps", Long.parseLong(tokenizedLine[index++]));
		m.put("deny_bps", Long.parseLong(tokenizedLine[index++]));
	}

	private void parseManagementSystemLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module flag 6010,6041
		Integer moduleFlag = (Integer) m.get("module_flag");
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));
		String action = tokenizedLine[index++];
		if (action.equals("0"))
			action = "격리";
		else if (action.equals("1"))
			action = "웹리디렉션";
		else if (action.equals("2"))
			action = "세션차단";
		m.put("action", action);
		String userToken = tokenizedLine[index++];
		m.put("user", userToken.isEmpty() ? null : userToken);
		m.put("module_name", tokenizedLine[index++]);
		m.put("description", tokenizedLine[index++]);
		String instCode = tokenizedLine[index++];
		m.put("inst_code", instCode.isEmpty() ? null : instCode);

		if (moduleFlag == 6041)
			return;

		String userType = tokenizedLine[index++];
		if (userType.equals("0"))
			userType = "시스템";
		else if (userType.equals("1"))
			userType = "일반관리자";
		else if (userType.equals("2"))
			userType = "가상화관리자";
		else if (userType.equals("3"))
			userType = "중간관리자";
		m.put("user_type", userType);

		String alertId = tokenizedLine[index++];
		m.put("alert_id", alertId.isEmpty() ? null : Integer.parseInt(alertId));
		int length = tokenizedLine.length - 1;
		if (length >= index) {
			String logType = tokenizedLine[index++];
			m.put("log_type", logType.isEmpty() ? null : Integer.parseInt(logType));
		} else
			m.put("log_type", null);

		if (length >= index) {
			String logSubType = tokenizedLine[index++];
			m.put("log_sub_type", logSubType.isEmpty() ? null : Integer.parseInt(logSubType));
		} else
			m.put("log_sub_type", null);

		if (length >= index) {
			String userAddr = tokenizedLine[index++];
			m.put("user_addr", userAddr.isEmpty() ? null : userAddr);
		} else
			m.put("user_addr", null);
	}

	private void parseManagementOperaionLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module flag 3010
		// DPX1 1
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));
		String action = tokenizedLine[index++];
		if (action.equals("0"))
			action = "격리";
		else if (action.equals("1"))
			action = "웹리디렉션";
		else if (action.equals("2"))
			action = "세션차단";
		m.put("action", action);
		String userToken = tokenizedLine[index++];
		m.put("user", userToken.isEmpty() ? null : userToken);
		m.put("module_name", tokenizedLine[index++]);
		m.put("description", tokenizedLine[index++]);
	}

	private void parseManagementStatLogV2(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;

		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];
		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));
		String action = tokenizedLine[index++];
		if (action.equals("0"))
			action = "격리";
		else if (action.equals("1"))
			action = "웹리디렉션";
		else if (action.equals("2"))
			action = "세션차단";
		m.put("action", action);
		String userToken = tokenizedLine[index++];
		m.put("user", userToken.isEmpty() ? null : userToken);

		m.put("module_name", tokenizedLine[index++]);

		String[] msgs = tokenizeLine(tokenizedLine[index++], ",");
		List<String> columns = Arrays.asList("cpu", "mem", "hdd", "session", "in_data", "out_data", "in_pkt", "out_pkt", "ha");
		for (int i = 0; i < msgs.length; i++) {
			String[] tokens = tokenizeLine(msgs[i], ":");
			String key = tokens[0];
			String value = tokens[1];
			if (i < columns.size())
				key = columns.get(i);

			if (key.endsWith("data"))
				value = value.substring(0, value.indexOf("M"));
			else if (key.endsWith("pkt"))
				value = value.substring(0, value.indexOf("p"));
			if (key.equals("ha"))
				m.put(key, value);
			else
				m.put(key, Double.parseDouble(value));

		}
	}

	private void parseManagementStatLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module flag 3011
		// DPX1 100

		int index = 8;

		m.put("module_name", tokenizedLine[index++]);
		m.put("cpu", Integer.valueOf(tokenizedLine[index++]));
		m.put("mem", Integer.valueOf(tokenizedLine[index++]));
		m.put("hdd", Integer.valueOf(tokenizedLine[index++]));
		m.put("session", Integer.valueOf(tokenizedLine[index++]));
		m.put("in_data", Long.parseLong(tokenizedLine[index++]));
		m.put("out_data", Long.parseLong(tokenizedLine[index++]));
		m.put("in_pkt", Long.parseLong(tokenizedLine[index++]));
		m.put("out_pkt", Long.parseLong(tokenizedLine[index++]));
		m.put("ha", tokenizedLine[index++]);
	}

	private void parseManagementNetworkPortLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module flag 3012
		int index = 8;
		m.put("zone_name", tokenizedLine[index++]);
		String nifType = tokenizedLine[index++];
		if (nifType.equals("0"))
			nifType = "physical";
		else if (nifType.equals("1"))
			nifType = "bridge";
		m.put("nif_type", nifType);
		m.put("nif_name", tokenizedLine[index++]);

		List<String> columns = Arrays.asList("in_rx_tcp_bps", "in_rx_udp_bps", "in_rx_icmp_bps", "in_rx_etc_bps",
				"in_rx_total_bps", "in_rx_tcp_pps", "in_rx_udp_pps", "in_rx_icmp_pps", "in_rx_etc_pps", "in_rx_total_pps",
				"in_tx_tcp_bps", "in_tx_udp_bps", "in_tx_icmp_bps", "in_tx_etc_bps", "in_tx_total_bps", "in_tx_tcp_pps",
				"in_tx_udp_pps", "in_tx_icmp_pps", "in_tx_etc_pps", "in_tx_total_pps", "in_drop_tcp_bps", "in_drop_udp_bps",
				"in_drop_icmp_bps", "in_drop_etc_bps", "in_drop_total_bps", "in_drop_tcp_pps", "in_drop_udp_pps",
				"in_drop_icmp_pps", "in_drop_etc_pps", "in_drop_total_pps", "out_rx_tcp_bps", "out_rx_udp_bps",
				"out_rx_icmp_bps", "out_rx_etc_bps", "out_rx_total_bps", "out_rx_tcp_pps", "out_rx_udp_pps", "out_rx_icmp_pps",
				"out_rx_etc_pps", "out_rx_total_pps", "out_tx_tcp_bps", "out_tx_udp_bps", "out_tx_icmp_bps", "out_tx_etc_bps",
				"out_tx_total_bps", "out_tx_tcp_pps", "out_tx_udp_pps", "out_tx_icmp_pps", "out_tx_etc_pps", "out_tx_total_pps",
				"out_drop_tcp_bps", "out_drop_udp_bps", "out_drop_icmp_bps", "out_drop_etc_bps", "out_drop_total_bps",
				"out_drop_tcp_pps", "out_drop_udp_pps", "out_drop_icmp_pps", "out_drop_etc_pps", "out_drop_total_pps");

		for (String column : columns) {
			String value = tokenizedLine[index++];
			m.put(column, value.isEmpty() ? null : Long.parseLong(value));
		}
	}

	private void parseDpxIpsLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module flag 3020
		// DPX1 9
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));

		m.put("pkt_len", Integer.valueOf(tokenizedLine[index++]));

		String action = tokenizedLine[index++];
		if (action.equals("3001"))
			action = "차단";
		else if (action.equals("3002"))
			action = "시스템 격리";
		else if (action.equals("3003"))
			action = "허용";
		else if (action.equals("3006"))
			action = "세션 끊기";
		else if (action.equals("3007"))
			action = "출발지 IP기준 사용량 제한";
		else if (action.equals("3008"))
			action = "목적지 IP기준 사용량 제한";
		else if (action.equals("3009"))
			action = "DDos 공격 차단";
		else if (action.equals("3010"))
			action = "일괄차단";
		m.put("action", action);

		String userToken = tokenizedLine[index++];
		m.put("user", userToken.isEmpty() ? null : userToken);
		m.put("module_name", tokenizedLine[index++]);
		m.put("reason", tokenizedLine[index++]);
		m.put("nif", tokenizedLine[index++]);
		m.put("eth_protocol", tokenizedLine[index++]);
		m.put("src_mac", tokenizedLine[index++]);
		m.put("rule_id", Long.parseLong(tokenizedLine[index++]));
		m.put("vlan_id", tokenizedLine[index++]);

		String status = tokenizedLine[index++].toLowerCase();
		if (status.equals("s"))
			status = "최초 공격탐지";
		else if (status.equals("c"))
			status = "탐지 후 주기적인 이벤트 전송";
		else if (status.equals("f"))
			status = "공격 종료";
		m.put("status", status);

		String duration = tokenizedLine[index++];
		m.put("duration", duration.isEmpty() ? null : duration);
		m.put("msg", tokenizedLine[index++]);
		m.put("slice_seconds", Long.parseLong(tokenizedLine[index++]));
		m.put("threshold_packets", Long.parseLong(tokenizedLine[index++]));
		m.put("threshold_bytes", Long.parseLong(tokenizedLine[index++]));
		m.put("attack_rate", tokenizedLine[index++]);
		String zoneName = tokenizedLine[index++];
		m.put("zone_name", zoneName.isEmpty() ? null : zoneName);

		String attackId = tokenizedLine[index++];
		m.put("attack_id", attackId.isEmpty() ? null : attackId);
	}

	private void parseUntrustedTrafficBlockFilterStatusLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module flag 3030,3080,3120,3150
		// DPX1 101,107,108,109,110,111,113,114
		int index = 8;
		String duration = tokenizedLine[index++];
		m.put("duration", duration.isEmpty() ? null : duration);
		m.put("drop_tcp_pps", Long.parseLong(tokenizedLine[index++]));
		m.put("drop_tcp_bps", Long.parseLong(tokenizedLine[index++]));
		m.put("drop_udp_pps", Long.parseLong(tokenizedLine[index++]));
		m.put("drop_udp_bps", Long.parseLong(tokenizedLine[index++]));
		m.put("drop_icmp_pps", Long.parseLong(tokenizedLine[index++]));
		m.put("drop_icmp_bps", Long.parseLong(tokenizedLine[index++]));
		m.put("drop_etc_pps", Long.parseLong(tokenizedLine[index++]));
		m.put("drop_etc_bps", Long.parseLong(tokenizedLine[index++]));

		String zoneName = tokenizedLine[index++];
		m.put("zone_name", zoneName.isEmpty() ? null : zoneName);
		String attackId = tokenizedLine[index++];
		m.put("attack_id", attackId.isEmpty() ? null : attackId);

		String filterStatus = tokenizedLine[index++];
		if (filterStatus.equals("0"))
			filterStatus = "off";
		else if (filterStatus.equals("1"))
			filterStatus = "차단없음";
		else if (filterStatus.equals("2"))
			filterStatus = "차단중";
		else if (filterStatus.equals("3"))
			filterStatus = "허용보다 차단이 많음";
		m.put("filter_status", filterStatus.isEmpty() ? null : filterStatus);
	}

	private void parseUntrustedTrafficBlockFilterBlockLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module flag 3031,3041,3051,3061,3071,3121,3131,3141,3151,3191,3211 ||
		// 3181,3201
		Integer moduleFlag = (Integer) m.get("module_flag");

		int index = 8;
		String duration = tokenizedLine[index++];
		m.put("duration", duration.isEmpty() ? null : duration);
		m.put("protocol", tokenizedLine[index++]);
		m.put("src_ip", tokenizedLine[index++]);
		m.put("dst_ip", tokenizedLine[index++]);
		m.put("dst_port", Integer.parseInt(tokenizedLine[index++]));

		if (moduleFlag == 3181 || moduleFlag == 3201) {
			m.put("allow_packets", Long.parseLong(tokenizedLine[index++]));
			m.put("allow_bytes", Long.parseLong(tokenizedLine[index++]));
		} else {
			m.put("drop_packets", Long.parseLong(tokenizedLine[index++]));
			m.put("drop_bytes", Long.parseLong(tokenizedLine[index++]));
		}

		String zoneName = tokenizedLine[index++];
		m.put("zone_name", zoneName.isEmpty() ? null : zoneName);
	}

	private void parseNetworkProtectionbySegmentStatusLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module flag 3040,3090,3100,3110,3130,3140
		// DPX1 103,112
		int index = 8;
		String duration = tokenizedLine[index++];
		m.put("duration", duration.isEmpty() ? null : duration);
		m.put("drop_tcp_pps", Long.parseLong(tokenizedLine[index++]));
		m.put("drop_tcp_bps", Long.parseLong(tokenizedLine[index++]));
		m.put("drop_udp_pps", Long.parseLong(tokenizedLine[index++]));
		m.put("drop_udp_bps", Long.parseLong(tokenizedLine[index++]));
		m.put("drop_icmp_pps", Long.parseLong(tokenizedLine[index++]));
		m.put("drop_icmp_bps", Long.parseLong(tokenizedLine[index++]));
		m.put("drop_etc_pps", Long.parseLong(tokenizedLine[index++]));
		m.put("drop_etc_bps", Long.parseLong(tokenizedLine[index++]));
		m.put("allow_tcp_pps", Long.parseLong(tokenizedLine[index++]));
		m.put("allow_tcp_bps", Long.parseLong(tokenizedLine[index++]));
		m.put("allow_udp_pps", Long.parseLong(tokenizedLine[index++]));
		m.put("allow_udp_bps", Long.parseLong(tokenizedLine[index++]));
		m.put("allow_icmp_pps", Long.parseLong(tokenizedLine[index++]));
		m.put("allow_icmp_bps", Long.parseLong(tokenizedLine[index++]));
		m.put("allow_etc_pps", Long.parseLong(tokenizedLine[index++]));
		m.put("allow_etc_bps", Long.parseLong(tokenizedLine[index++]));

		String zoneName = tokenizedLine[index++];
		m.put("zone_name", zoneName.isEmpty() ? null : zoneName);
		String attackId = tokenizedLine[index++];
		m.put("attack_id", attackId.isEmpty() ? null : attackId);
		String filterStatus = tokenizedLine[index++];
		if (filterStatus.equals("0"))
			filterStatus = "off";
		else if (filterStatus.equals("1"))
			filterStatus = "차단없음";
		else if (filterStatus.equals("2"))
			filterStatus = "차단중";
		else if (filterStatus.equals("3"))
			filterStatus = "허용보다 차단이 많음";
		m.put("filter_status", filterStatus.isEmpty() ? null : filterStatus);
	}

	private void parseAntiSpoofingProtectionStatusLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module flag 3050,3060,3210
		// DPX1 104, 105
		int index = 8;
		String duration = tokenizedLine[index++];
		m.put("duration", duration.isEmpty() ? null : duration);
		m.put("block_session", tokenizedLine[index++]);
		m.put("allow_session", tokenizedLine[index++]);

		String zoneName = tokenizedLine[index++];
		m.put("zone_name", zoneName.isEmpty() ? null : zoneName);
		String attackId = tokenizedLine[index++];
		m.put("attack_id", attackId.isEmpty() ? null : attackId);
		String filterStatus = tokenizedLine[index++];
		if (filterStatus.equals("0"))
			filterStatus = "off";
		else if (filterStatus.equals("1"))
			filterStatus = "차단없음";
		else if (filterStatus.equals("2"))
			filterStatus = "차단중";
		else if (filterStatus.equals("3"))
			filterStatus = "허용보다 차단이 많음";
		m.put("filter_status", filterStatus.isEmpty() ? null : filterStatus);
	}

	private void parseAntiSpoofingProtectionAuthLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module_flag 3052,3212,3062
		Integer moduleFlag = (Integer) m.get("module_flag");
		int index = 8;
		String duration = tokenizedLine[index++];
		m.put("duration", duration.isEmpty() ? null : duration);
		m.put("protocol", tokenizedLine[index++]);
		m.put("src_ip", tokenizedLine[index++]);
		m.put("dst_ip", tokenizedLine[index++]);
		m.put("dst_port", Integer.parseInt(tokenizedLine[index++]));
		if (moduleFlag == 3062)
			m.put("request_count", Integer.parseInt(tokenizedLine[index++]));
		else
			m.put("connection_count", Integer.parseInt(tokenizedLine[index++]));
		m.put("expire_time", tokenizedLine[index++] + " " + tokenizedLine[index++]);

		String zoneName = tokenizedLine[index++];
		m.put("zone_name", zoneName.isEmpty() ? null : zoneName);
	}

	private void parseStatefulPacketInspectionStatusLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module_flag 3070
		// DPX1 106
		int index = 8;
		String duration = tokenizedLine[index++];
		m.put("duration", duration.isEmpty() ? null : duration);
		m.put("drop_tcp_pps", Long.parseLong(tokenizedLine[index++]));
		m.put("allow_tcp_pps", Long.parseLong(tokenizedLine[index++]));

		String zoneName = tokenizedLine[index++];
		m.put("zone_name", zoneName.isEmpty() ? null : zoneName);
		String attackId = tokenizedLine[index++];
		m.put("attack_id", attackId.isEmpty() ? null : attackId);
		String filterStatus = tokenizedLine[index++];
		if (filterStatus.equals("0"))
			filterStatus = "off";
		else if (filterStatus.equals("1"))
			filterStatus = "차단없음";
		else if (filterStatus.equals("2"))
			filterStatus = "차단중";
		else if (filterStatus.equals("3"))
			filterStatus = "허용보다 차단이 많음";
		m.put("filter_status", filterStatus.isEmpty() ? null : filterStatus);
	}

	private void parseSegmentProtectionFilterStatusLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module_flag 3160
		// DPX1 201
		int index = 8;
		String duration = tokenizedLine[index++];
		m.put("duration", duration.isEmpty() ? null : duration);
		m.put("ip", tokenizedLine[index++]);
		m.put("mask", Integer.parseInt(tokenizedLine[index++]));
		m.put("rate_limit", tokenizedLine[index++]);
		String zoneName = tokenizedLine[index++];
		m.put("zone_name", zoneName.isEmpty() ? null : zoneName);
	}

	private void parseAttackLogV3(String[] tokenizedLine, Map<String, Object> m) {
		// module_flag 3170,3171
		// DPX1 210, 211
		int index = 8;
		String duration = tokenizedLine[index++];
		m.put("duration", duration.isEmpty() ? null : duration);
		String zoneName = tokenizedLine[index++];
		m.put("zone_name", zoneName.isEmpty() ? null : zoneName);
		String attackId = tokenizedLine[index++];
		m.put("attack_id", attackId.isEmpty() ? null : attackId);
	}

	private void parseSystemQuarantineLogV3(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));
		String action = tokenizedLine[index++];
		if (action.equals("0"))
			action = "격리";
		else if (action.equals("1"))
			action = "웹리디렉션";
		else if (action.equals("2"))
			action = "세션차단";
		m.put("action", action);
		String userToken = tokenizedLine[index++];
		m.put("user", userToken.isEmpty() ? null : userToken);
		m.put("module_name", tokenizedLine[index++]);
		m.put("description", tokenizedLine[index++]);
		String codeToken = tokenizedLine[index++];
		m.put("code", codeToken.isEmpty() ? null : codeToken);
	}

	private void parseProxyLogV3(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));
		String action = tokenizedLine[index++];
		if (action.equals("3003"))
			action = "ACT_PASS";
		else if (action.equals("3001"))
			action = "ACT_DROP";
		m.put("action", action);
		String userToken = tokenizedLine[index++];
		m.put("user", userToken.isEmpty() ? null : userToken);
		m.put("module_name", tokenizedLine[index++]);
		m.put("description", tokenizedLine[index++]);
		String codeToken = tokenizedLine[index++];
		m.put("code", codeToken.isEmpty() ? null : codeToken);
	}

	private void parseLbqosLogV3(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));
		m.put("action", tokenizedLine[index++]);
		index++;// user not use
		m.put("module_name", tokenizedLine[index++]);
		m.put("description", tokenizedLine[index++]);
		String codeToken = tokenizedLine[index++];
		m.put("code", codeToken.isEmpty() ? null : codeToken);
	}

	private void parseQosLogV3(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));
		m.put("action", tokenizedLine[index++]);
		String userToken = tokenizedLine[index++];
		m.put("user", userToken.isEmpty() ? null : userToken);
		m.put("module_name", tokenizedLine[index++]);
		m.put("qos_name", tokenizedLine[index++]);
		m.put("eth_name", tokenizedLine[index++]);
		m.put("bps", Integer.valueOf(tokenizedLine[index++]));
		m.put("pps", Integer.valueOf(tokenizedLine[index++]));
		String codeToken = tokenizedLine[index++];
		m.put("code", codeToken.isEmpty() ? null : codeToken);
	}

	private void parseInternetAccessControlLogV3(String[] tokenizedLine, Integer moduleFlag, Map<String, Object> m) {
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));
		String action = tokenizedLine[index++];
		if (action.equals("0"))
			action = "허용";
		else if (action.equals("1"))
			action = "차단";
		else if (action.equals("2"))
			action = "설치유도";
		else if (action.equals("3"))
			action = "삭제";
		else if (action.equals("4"))
			action = "차단(미설치)";
		else if (action.equals("10"))
			action = "악성패킷차단요청";
		else if (action.equals("11"))
			action = "치료상태감시중";
		else if (action.equals("12"))
			action = "안리포트수집요청";
		m.put("action", action);
		String userToken = tokenizedLine[index++];
		m.put("user", userToken.isEmpty() ? null : userToken);
		m.put("module_name", tokenizedLine[index++]);
		if (moduleFlag == 1120)
			m.put("mac", tokenizedLine[index++]);
		else if (moduleFlag == 1121)
			m.put("group_name", tokenizedLine[index++]);

		String codeToken = tokenizedLine[index++];
		m.put("code", codeToken.isEmpty() ? null : codeToken);
	}

	private void parseDnsLogV3(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));
		m.put("action", tokenizedLine[index++]);
		index++;// user not use
		m.put("module_name", tokenizedLine[index++]);
		m.put("reason", tokenizedLine[index++]);
		m.put("description", tokenizedLine[index++]);

		String codeToken = tokenizedLine[index++];
		m.put("code", codeToken.isEmpty() ? null : codeToken);
	}

	private void parseIpsLogV3(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));

		String action = tokenizedLine[index++];
		if (action.equals("3003"))
			action = "허용";
		else if (action.equals("3001"))
			action = "차단";
		else if (action.equals("3002"))
			action = "시스템 격리";
		else if (action.equals("3006"))
			action = "세션 끊기";
		else if (action.equals("3007"))
			action = "사용량 제한";
		else if (action.equals("3008"))
			action = "DDos차단";

		m.put("action", action);
		String userToken = tokenizedLine[index++];
		m.put("user", userToken.isEmpty() ? null : userToken);
		m.put("module_name", tokenizedLine[index++]);
		m.put("reason", tokenizedLine[index++]);
		m.put("nif", tokenizedLine[index++]);
		m.put("eth_protocol", tokenizedLine[index++]);
		m.put("src_mac", tokenizedLine[index++]);
		m.put("rule_id", tokenizedLine[index++]);
		m.put("vlan_id", tokenizedLine[index++]);
		m.put("msg", tokenizedLine[index++]);

		String codeToken = tokenizedLine[index++];
		m.put("code", codeToken.isEmpty() ? null : codeToken);
	}

	private void parseAppFilterLogV3(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));
		m.put("action", tokenizedLine[index++]);
		String userToken = tokenizedLine[index++];
		m.put("user", userToken.isEmpty() ? null : userToken);
		m.put("module_name", tokenizedLine[index++]);
		m.put("ap_protocol", tokenizedLine[index++]);
		m.put("description", tokenizedLine[index++]);

		String codeToken = tokenizedLine[index++];
		m.put("code", codeToken.isEmpty() ? null : codeToken);
	}

	private void parseWebFilterLogV3(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		String severityToken = tokenizedLine[index++];
		String protocolToken = tokenizedLine[index++];

		m.put("severity", severityToken.isEmpty() ? null : Integer.valueOf(severityToken));
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", tokenizedLine[index++]);
		m.put("src_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("dst_ip", tokenizedLine[index++]);
		m.put("dst_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("action", tokenizedLine[index++]);
		String userToken = tokenizedLine[index++];
		m.put("user", userToken.isEmpty() ? null : userToken);
		m.put("module_name", tokenizedLine[index++]);
		m.put("wf_type", tokenizedLine[index++]);
		m.put("reason", tokenizedLine[index++]);
		m.put("url", tokenizedLine[index++]);

		String codeToken = tokenizedLine[index++];
		m.put("code", codeToken.isEmpty() ? null : codeToken);
	}

	private void parseStatLogV3(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;

		m.put("module_name", tokenizedLine[index++]);
		m.put("cpu", Integer.valueOf(tokenizedLine[index++]));
		m.put("mem", Integer.valueOf(tokenizedLine[index++]));
		m.put("hdd", Integer.valueOf(tokenizedLine[index++]));
		m.put("session", tokenizedLine[index++]);
		m.put("in_data", Long.parseLong(tokenizedLine[index++]));
		m.put("out_data", Long.parseLong(tokenizedLine[index++]));
		m.put("in_pkt", Long.parseLong(tokenizedLine[index++]));
		m.put("out_pkt", Long.parseLong(tokenizedLine[index++]));
		m.put("ha", tokenizedLine[index++]);
		String codeToken = tokenizedLine[index++];
		m.put("code", codeToken.isEmpty() ? null : codeToken);
	}

	private void parseOperationLogV3(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		String severityToken = tokenizedLine[index++];
		m.put("severity", severityToken.isEmpty() ? null : Integer.valueOf(severityToken));

		String protocolToken = tokenizedLine[index++];
		String srcIpToken = tokenizedLine[index++];
		String srcPortToken = tokenizedLine[index++];
		String dstIpToken = tokenizedLine[index++];
		String dstPortToken = tokenizedLine[index++];

		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", srcIpToken.isEmpty() ? null : srcIpToken);
		m.put("src_port", srcPortToken.isEmpty() ? null : Integer.valueOf(srcPortToken));
		m.put("dst_ip", dstIpToken.isEmpty() ? null : dstIpToken);
		m.put("dst_port", dstPortToken.isEmpty() ? null : Integer.valueOf(dstPortToken));

		m.put("action", tokenizedLine[index++]);
		String userToken = tokenizedLine[index++];
		m.put("user", userToken.isEmpty() ? null : userToken);
		m.put("module_name", tokenizedLine[index++]);
		m.put("description", tokenizedLine[index++]);
		String codeToken = tokenizedLine[index++];
		m.put("code", codeToken.isEmpty() ? null : codeToken);
	}

	private void parseFirewallLogV3(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		Integer moduleFlag = (Integer) m.get("module_flag");
		if (moduleFlag == 6041) {
			parseManagementSystemLogV3(tokenizedLine, m);
			return;
		}
		String logType = tokenizedLine[index++];
		if (logType.equals("1")) {
			logType = "Allow";
		} else if (logType.equals("2")) {
			logType = "Deny";
		} else if (logType.equals("3")) {
			logType = "Expire";
		} else if (logType.equals("4")) {
			logType = "Alive";
		}
		m.put("logtype", logType);
		String protocolToken = tokenizedLine[index++];
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("policy_id", tokenizedLine[index++]);
		m.put("src_ip", tokenizedLine[index++]);
		m.put("src_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("dst_ip", tokenizedLine[index++]);
		m.put("dst_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("in_nic", tokenizedLine[index++]);
		m.put("out_nic", tokenizedLine[index++]);

		String natTypeToken = tokenizedLine[index++];
		String natIp = tokenizedLine[index++];
		String natPortToken = tokenizedLine[index++];

		if (moduleFlag == 6020 || moduleFlag == 6021) {
			m.put("snat_type", natTypeToken.isEmpty() ? null : natTypeToken);
			m.put("snat_ip", natIp.isEmpty() ? null : natIp);
			m.put("snat_port", natPortToken.isEmpty() ? null : Integer.valueOf(natPortToken));
		} else {
			m.put("nat_type", natTypeToken.isEmpty() ? null : natTypeToken);
			m.put("nat_ip", natIp.isEmpty() ? null : natIp);
			m.put("nat_port", natPortToken.isEmpty() ? null : Integer.valueOf(natPortToken));
		}

		String sentDataToken = tokenizedLine[index++];
		String sentPktToken = tokenizedLine[index++];
		String rcvdDataToken = tokenizedLine[index++];
		String rcvdPktToken = tokenizedLine[index++];
		String duration = tokenizedLine[index++];
		String state = tokenizedLine[index++];
		String reason = tokenizedLine[index++];
		String code = tokenizedLine[index++];
		String tcpFlag = tokenizedLine[index++];

		m.put("sent_data", sentDataToken.isEmpty() ? null : Long.valueOf(sentDataToken));
		m.put("sent_pkt", sentPktToken.isEmpty() ? null : Long.valueOf(sentPktToken));
		m.put("rcvd_data", rcvdDataToken.isEmpty() ? null : Long.valueOf(rcvdDataToken));
		m.put("rcvd_pkt", rcvdPktToken.isEmpty() ? null : Long.valueOf(rcvdPktToken));
		m.put("duration", duration.isEmpty() ? null : duration);
		m.put("state", state.isEmpty() ? null : state);
		m.put("reason", reason.isEmpty() ? null : reason);
		if (moduleFlag == 6020 || moduleFlag == 6021)
			m.put("inst_code", code.isEmpty() ? null : code);
		else
			m.put("code", code.isEmpty() ? null : code);
		m.put("tcp_flag", tcpFlag.isEmpty() ? null : tcpFlag);

		if (moduleFlag == 6020 || moduleFlag == 6021) {
			String inZone = tokenizedLine[index++];
			m.put("in_zone", inZone.isEmpty() ? null : inZone);
			String outZone = tokenizedLine[index++];
			m.put("out_zone", outZone.isEmpty() ? null : outZone);

			m.put("rule_id", Long.parseLong(tokenizedLine[index++]));
			m.put("nat_id", tokenizedLine[index++]);
			m.put("ip_ver", Integer.parseInt(tokenizedLine[index++]));

			String dnatTypeToken = tokenizedLine[index++];
			String dnatIp = tokenizedLine[index++];
			String dnatPortToken = tokenizedLine[index++];
			m.put("dnat_type", dnatTypeToken.isEmpty() ? null : dnatTypeToken);
			m.put("dnat_ip", dnatIp.isEmpty() ? null : dnatIp);
			m.put("dnat_port", dnatPortToken.isEmpty() ? null : Integer.valueOf(dnatPortToken));
		}
	}

	private void parseFirewallLogV1(String[] tokenizedLine, Map<String, Object> m) {
		int index = 5;
		// log data
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
		String dateToken = tokenizedLine[index++];
		String timeToken = tokenizedLine[index++];
		try {
			m.put("date", dateFormat.parse(dateToken + " " + timeToken));
		} catch (ParseException e) {
		}

		String logType = tokenizedLine[index++];
		m.put("logtype", logType);

		String protocolToken = tokenizedLine[index++];
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("policy_id", tokenizedLine[index++]);
		m.put("src_ip", tokenizedLine[index++]);
		m.put("src_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("dst_ip", tokenizedLine[index++]);
		m.put("dst_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("in_nic", tokenizedLine[index++]);
		m.put("out_nic", tokenizedLine[index++]);

		String natTypeToken = tokenizedLine[index++];
		m.put("nat_type", natTypeToken.isEmpty() ? null : natTypeToken);

		String natIp = tokenizedLine[index++];
		m.put("nat_ip", natIp.isEmpty() ? null : natIp);

		String natPortToken = tokenizedLine[index++];
		m.put("nat_port", natPortToken.isEmpty() ? null : Integer.valueOf(natPortToken));

		String sentDataToken = tokenizedLine[index++];
		String sentPktToken = tokenizedLine[index++];
		String rcvdDataToken = tokenizedLine[index++];
		String rcvdPktToken = tokenizedLine[index++];

		m.put("sent_data", sentDataToken.isEmpty() ? null : Long.valueOf(sentDataToken));
		m.put("sent_pkt", sentPktToken.isEmpty() ? null : Long.valueOf(sentPktToken));
		m.put("rcvd_data", rcvdDataToken.isEmpty() ? null : Long.valueOf(rcvdDataToken));
		m.put("rcvd_pkt", rcvdPktToken.isEmpty() ? null : Long.valueOf(rcvdPktToken));
	}

	private void parseApplicationLogV1(String[] tokenizedLine, Map<String, Object> m) {
		int index = 5;
		int moduleFlag = Integer.valueOf(tokenizedLine[index++]);
		m.put("module_flag", moduleFlag);

		// log data
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
		String dateToken = tokenizedLine[index++];
		String timeToken = tokenizedLine[index++];
		try {
			m.put("date", dateFormat.parse(dateToken + " " + timeToken));
		} catch (ParseException e) {
		}

		if (moduleFlag == 1)
			parseOperationLogV1(tokenizedLine, m);
		else if (moduleFlag == 2)
			parseVirusLogV1(tokenizedLine, m);
		else if (moduleFlag == 3)
			parseSpamLogV1(tokenizedLine, m);
		else if (moduleFlag == 4)
			parseWebFilterLogV1(tokenizedLine, m);
		else if (moduleFlag == 6)
			parseAppFilterLogV1(tokenizedLine, m);
		else if (moduleFlag == 8)
			parseSslVpnLogV1(tokenizedLine, m);
		else if (moduleFlag == 9)
			parseIpsLogV1(tokenizedLine, m);
		else if (moduleFlag == 12)
			parseInternetAccessControlLogV1(tokenizedLine, m);
	}

	private void parseOperationLogV1(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		String severityToken = tokenizedLine[index++];
		index++;

		m.put("severity", severityToken);
		index++;
		index++;
		m.put("action", tokenizedLine[index++]);
		index++;
		m.put("module_name", tokenizedLine[index++]);
		m.put("description", tokenizedLine[index++]);
	}

	private void parseVirusLogV1(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		String severityToken = tokenizedLine[index++];
		String protocolToken = tokenizedLine[index++];

		m.put("severity", severityToken);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", tokenizedLine[index++]);
		m.put("src_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("dst_ip", tokenizedLine[index++]);
		m.put("dst_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("action", tokenizedLine[index++]);
		m.put("user", tokenizedLine[index++]);
		m.put("module_name", tokenizedLine[index++]);
		m.put("virus_filter", tokenizedLine[index++]);
		m.put("virus_name", tokenizedLine[index++]);

		String path = tokenizedLine[index++];
		if (path.startsWith("[") && path.endsWith("]"))
			m.put("virus_url", path.substring(1, path.length() - 1));
		else
			m.put("virus_fname", path);

		if (tokenizedLine.length > 21) {
			m.put("sender_addr", tokenizedLine[index++]);
			m.put("recipients_addr", tokenizedLine[index++]);
			m.put("subject", tokenizedLine[index++]);
		}
	}

	private void parseSpamLogV1(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		String severityToken = tokenizedLine[index++];
		String protocolToken = tokenizedLine[index++];

		m.put("severity", severityToken);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", tokenizedLine[index++]);
		m.put("src_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("dst_ip", tokenizedLine[index++]);
		m.put("dst_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("action", tokenizedLine[index++]);
		m.put("user", tokenizedLine[index++]);
		m.put("module_name", tokenizedLine[index++]);

		m.put("spam_filter", tokenizedLine[index++]);
		m.put("send_spam_log", tokenizedLine[index++]);
		m.put("sender_addr", tokenizedLine[index++]);
		m.put("recipients_addr", tokenizedLine[index++]);
		m.put("subject", tokenizedLine[index++]);
	}

	private void parseWebFilterLogV1(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		String severityToken = tokenizedLine[index++];
		String protocolToken = tokenizedLine[index++];

		m.put("severity", severityToken);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", tokenizedLine[index++]);
		m.put("src_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("dst_ip", tokenizedLine[index++]);
		m.put("dst_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("action", tokenizedLine[index++]);
		m.put("user", tokenizedLine[index++]);
		m.put("module_name", tokenizedLine[index++]);
		m.put("wf_type", tokenizedLine[index++]);
		m.put("reason", tokenizedLine[index++]);

		String url = tokenizedLine[index++];
		m.put("url", url.substring(1, url.length() - 1));
	}

	private void parseAppFilterLogV1(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src", tokenizedLine[index++]);
		m.put("dst", tokenizedLine[index++]);
		m.put("action", tokenizedLine[index++]);
		m.put("user", tokenizedLine[index++]);
		m.put("module_name", tokenizedLine[index++]);
		m.put("ap_protocol", tokenizedLine[index++]);
		m.put("description", tokenizedLine[index++]);
	}

	private void parseSslVpnLogV1(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", tokenizedLine[index++]);
		m.put("src_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("dst_ip", tokenizedLine[index++]);
		m.put("dst_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("action", tokenizedLine[index++]);
		m.put("user", tokenizedLine[index++]);
		m.put("module_name", tokenizedLine[index++]);
		m.put("event", tokenizedLine[index++]);
		m.put("epsec", tokenizedLine[index++]);
	}

	private void parseIpsLogV1(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", tokenizedLine[index++]);
		m.put("src_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("dst_ip", tokenizedLine[index++]);
		m.put("dst_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("action", tokenizedLine[index++]);
		m.put("user", tokenizedLine[index++]);
		m.put("module_name", tokenizedLine[index++]);
		m.put("reason", tokenizedLine[index++]);
		m.put("nif", tokenizedLine[index++]);
		m.put("eth_protocol", tokenizedLine[index++]);
		m.put("src_mac", tokenizedLine[index++]);
		m.put("rule_id", tokenizedLine[index++]);
		m.put("vlan_id", tokenizedLine[index++]);
		m.put("msg", tokenizedLine[index++]);
	}

	private void parseInternetAccessControlLogV1(String[] tokenizedLine, Map<String, Object> m) {
		int index = 8;
		int severity = Integer.valueOf(tokenizedLine[index++]);
		String protocolToken = tokenizedLine[index++];

		m.put("severity", severity);
		m.put("protocol", protocolToken.isEmpty() ? null : protocolToken);
		m.put("src_ip", tokenizedLine[index++]);
		m.put("src_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("dst_ip", tokenizedLine[index++]);
		m.put("dst_port", Integer.valueOf(tokenizedLine[index++]));
		m.put("action", tokenizedLine[index++]);
		m.put("user", tokenizedLine[index++]);
		m.put("module_name", tokenizedLine[index++]);
		m.put("mac", tokenizedLine[index++]);
	}

	private String[] tokenizeLine(String line, String delimiter) {
		int last = 0;
		List<String> tokenizedLine = new ArrayList<String>(32);
		while (true) {
			int p = line.indexOf(delimiter, last);

			String token = null;
			if (p >= 0)
				token = line.substring(last, p);
			else
				token = line.substring(last);

			tokenizedLine.add(token);

			if (p < 0)
				break;
			last = ++p;
		}

		return tokenizedLine.toArray(new String[0]);
	}
}
