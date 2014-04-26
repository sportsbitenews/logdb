/*
 * Copyright 2014 Eediom Inc.
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
package org.araqne.logparser.syslog.piolink;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * @author kyun
 */
public class WebFrontLogParserTest {

	@Test
	public void testParser1() {
		String line = "(warn) kernel: [WEBFRONT/0x00721004] Violated Access Control - Requested URL is in the Block URL List (log_id=\"800453\",app_name=\"9_ennavi_co_kr\",app_id=\"9\",src_if=\"aaa\",src_ip=\"208.115.113.84\",src_port=\"46511\",dest_ip=\"211.200.15.185\",dest_port=\"80\",forwarded_for=\"\",host=\"ennavi.co.kr\",url=\"/robots.txt\",sig_warning=\"Middle\",url_param=\"\",block=\"yes\",owasp=\"A8\",sigid=\"110100016\")"; 

		WebFrontLogParser parser = new WebFrontLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("warn", m.get("level"));
		assertEquals("0x00721004", m.get("event_id"));
		assertEquals("Violated Access Control - Requested URL is in the Block URL List", m.get("event_str"));
		assertEquals("800453",m.get("log_id"));
		assertEquals("9_ennavi_co_kr",m.get("app_name"));
		assertEquals("9",m.get("app_id"));
		assertEquals("aaa" ,m.get("src_if"));
		assertEquals("208.115.113.84" ,m.get("src_ip"));
		assertEquals("46511" ,m.get("src_port"));
		assertEquals("211.200.15.185" ,m.get("dest_ip"));
		assertEquals("80" ,m.get("dest_port"));
		assertEquals("" ,m.get("forwarded_for"));
		assertEquals("ennavi.co.kr" ,m.get("host"));
		assertEquals("/robots.txt" ,m.get("url"));
		assertEquals("Middle" ,m.get("sig_warning"));
		assertEquals("" ,m.get("url_param"));
		assertEquals("yes" ,m.get("block"));
		assertEquals("A8" ,m.get("owasp"));
		assertEquals("110100016" ,m.get("sigid"));
	}

	@Test
	public void testParser2() {
		String line = "(warn) kernel: [WEBFRONT/0x00D01001] Evidence (log_id=\"1074568587\",app_name=\"9_ennavi_co_kr\",app_id=\"9\",id=\"1073854532\",data=\"0180 54 20 35 2e 31 29 20 41 70 70 6c 65 57 65 62 4b T 5.1) A ppleWebK^I0190 69 74 2f 35 33 37 2e 33 36 20 28 4b 48 54 4d 4c it/537.3 6 (KHTML^I01a0 2c 20 6c 69 6b 65 20 47 65 63 6b 6f 29 20 43 68 , like G ecko) Ch^I01b0 72 6f 6d 65 2f 33 31 2e 30 2e 31 36 35 30 2e 34 rome/31. 0.1650.4^I01c0 38 20 53 61 66 61 72 69 2f 35 33 37 2e 33 36 0d 8 Safari /537.36.^I01d0 0a 41 63 63 65 70 74 2d 45 6e 63 6f 64 69 6e 67 .Accept- Encoding^I01e0 3a 20 67 7a 69 70 2c 64 65 66 6c 61 74 65 0d 0a : gzip,d eflate..^I01f0 0d 0a ..^I\")";
		WebFrontLogParser parser = new WebFrontLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("warn", m.get("level"));
		assertEquals("0x00D01001", m.get("event_id"));
		assertEquals("Evidence", m.get("event_str"));
		assertEquals("1074568587", m.get("log_id"));
		assertEquals("9_ennavi_co_kr", m.get("app_name"));
		assertEquals("9", m.get("app_id"));
		assertEquals("1073854532", m.get("id"));
		assertEquals("0180 54 20 35 2e 31 29 20 41 70 70 6c 65 57 65 62 4b T 5.1) A ppleWebK^I0190 69 74 2f 35 33 37 2e 33 36 20 28 4b 48 54 4d 4c it/537.3 6 (KHTML^I01a0 2c 20 6c 69 6b 65 20 47 65 63 6b 6f 29 20 43 68 , like G ecko) Ch^I01b0 72 6f 6d 65 2f 33 31 2e 30 2e 31 36 35 30 2e 34 rome/31. 0.1650.4^I01c0 38 20 53 61 66 61 72 69 2f 35 33 37 2e 33 36 0d 8 Safari /537.36.^I01d0 0a 41 63 63 65 70 74 2d 45 6e 63 6f 64 69 6e 67 .Accept- Encoding^I01e0 3a 20 67 7a 69 70 2c 64 65 66 6c 61 74 65 0d 0a : gzip,d eflate..^I01f0 0d 0a ..^I"
				,m.get("data"));

	}
	
	@Test
	public void testParser3() {
		String line = "(warn) kernel: [WEBFRONT/0x0072F001] Violated Web Attack Tool Prevention - the request isn't allowed. "
				+ "(log_id=\"15282\",app_name=\"10_boheom_net\",app_id=\"10\",src_if=\"aaa\",src_ip=\"65.55.24.238\",src_port=\"22740\","
				+ "dest_ip=\"211.200.12.10\",dest_port=\"80\",forwarded_for=\"\",host=\"insucop.com\",url=\"/insucop_new/inbound/mail/eiB060.do\","
				+ "sig_warning=\"Low\",url_param=\"?flag=403&email=thebrain@naver.com&memberid=1771954\",block=\"yes\",evidence_id=\"2636\",owasp=\"A4\"\","
				+ "sigid=\"111500052\")";

		WebFrontLogParser parser = new WebFrontLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("warn", m.get("level"));
		assertEquals("0x0072F001", m.get("event_id"));
		assertEquals("Violated Web Attack Tool Prevention - the request isn't allowed.", m.get("event_str"));
		assertEquals("15282", m.get("log_id"));
		assertEquals("10_boheom_net", m.get("app_name"));
		assertEquals("10", m.get("app_id"));
		assertEquals("aaa", m.get("src_if"));
		assertEquals("65.55.24.238", m.get("src_ip"));
		assertEquals("22740", m.get("src_port"));
		assertEquals("211.200.12.10", m.get("dest_ip"));
		
		assertEquals("", m.get("forwarded_for"));
		assertEquals("insucop.com", m.get("host"));
		assertEquals("/insucop_new/inbound/mail/eiB060.do", m.get("url"));
		assertEquals("Low", m.get("sig_warning"));
		
		
		assertEquals("?flag=403&email=thebrain@naver.com&memberid=1771954", m.get("url_param"));
		assertEquals("yes", m.get("block"));
		assertEquals("2636", m.get("evidence_id"));
		assertEquals("A4\"", m.get("owasp"));
		assertEquals("111500052", m.get("sigid"));
	}
	
	@Test
	public void testParser4() {
		String line = "(warn) kernel: [WEBFRONT/0x00D01001] Evidence "
				+ "(log_id=\"15268\",app_name=\" 4_impay\",app_id=\"4\",id=\"2635\",data=\"0000 00 00 00 00 0c 07 ac 05 00 10 db ff 21 60 81 00 ........ ....!`..^I0010 00 "
				+ "0a 08 00 45 00 02 80 58 18 40 00 71 06 e4 eb ....E... X.@.q...^I0020 7c 42 b8 04 cb eb ca 41 ce 9a 00 50 25 c6 9e 8a |B.....A ...P%%"
				+ "...^I0030 52 6c e9 a9 50 18 fd 5c 20 22 00 00 47 45 54 20 Rl..P.. \"..GET ^I0040 2f 2e 2f 6"
				+ "a 73 2f 61 2e 75 72 6c 2c 64 2e 6f 6e /./js/a. url,d.on^I0050 6c 6f 61 64 3d 64 2e 6f 6e 72 65 61 64 79 73 74 load=d.o nreadyst^I0060 61 74 65 63 68 61 6e 67 65 3d 66 75 6e 63 74 69 atechang e=functi^I0070 6f 6e 61 2c 63 7b 69 66 63 7c 7c 21 64 2e 72 65 ona,c{if c||!d.re^I\")"; 
	
		WebFrontLogParser parser = new WebFrontLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("warn", m.get("level"));
		assertEquals("0x00D01001", m.get("event_id"));
		assertEquals("Evidence", m.get("event_str"));
		assertEquals("15268", m.get("log_id"));
		assertEquals("4_impay", m.get("app_name"));
		assertEquals("4", m.get("app_id"));
		assertEquals("2635", m.get("id"));
		assertEquals("0000 00 00 00 00 0c 07 ac 05 00 10 db ff 21 60 81 00 ........ ....!`..^I0010 00 "
				+ "0a 08 00 45 00 02 80 58 18 40 00 71 06 e4 eb ....E... X.@.q...^I0020 7c 42 b8 04 cb eb ca 41 ce 9a 00 50 25 c6 9e 8a |B.....A ...P%%"
				+ "...^I0030 52 6c e9 a9 50 18 fd 5c 20 22 00 00 47 45 54 20 Rl..P.. \"..GET ^I0040 2f 2e 2f 6"
				+ "a 73 2f 61 2e 75 72 6c 2c 64 2e 6f 6e /./js/a. url,d.on^I0050 6c 6f 61 64 3d 64 2e 6f 6e 72 65 61 64 79 73 74 load=d.o nreadyst^I0060 61 74 65 63 68 61 6e 67 65 3d 66 75 6e 63 74 69 atechang e=functi^I0070 6f 6e 61 2c 63 7b 69 66 63 7c 7c 21 64 2e 72 65 ona,c{if c||!d.re^I", m.get("data"));
	}
	
	
	@Test
	public void testParser5() {
		String line = "(notice) ntp_client: [WEBFRONT/0x0018D001] Message from NTP Client (msg=\"Trying to adjust time.\")";
		
		WebFrontLogParser parser = new WebFrontLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("notice", m.get("level"));
		assertEquals("0x0018D001", m.get("event_id"));
		assertEquals("Message from NTP Client", m.get("event_str"));
		assertEquals("Trying to adjust time.", m.get("msg"));
		
	}
	
	@Test
	public void testParser6() {
		String line = "(warn) kernel: [WEBFRONT/0x0072F001] Violated Web Attack Tool Prevention - the request isn't allowed. "
				+ "(log_id=\"7774\",app_name=\"37_ors_tillion\",app_id=\"11\",src_if=\"waf\",src_ip=\"66.249.67.212\",src_port=\"45348\","
				+ "dest_ip=\"203.235.202.37\",dest_port=\"80\",forwarded_for=\"\",host=\"ors.tillion.co.kr\",url=\"/Survey/DisplayStartMessage.aspx\","
				+ "sig_warning=\"Middle\","
				+ "url_param=\"?SurveyID=2011321&RespondentID=&GroupID=1&QID=&AthSiID=\","
				+ "block=\"no\",evidence_id=\"3770\","
				+ "owasp=\"A4\"\",,"
				+ "sigid=\"111500008\")";
		
		
		WebFrontLogParser parser = new WebFrontLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("warn", m.get("level"));
		assertEquals("0x0072F001", m.get("event_id"));
		assertEquals("Violated Web Attack Tool Prevention - the request isn't allowed.", m.get("event_str"));
		assertEquals("7774", m.get("log_id"));
		
	}
	
	
	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}

}
