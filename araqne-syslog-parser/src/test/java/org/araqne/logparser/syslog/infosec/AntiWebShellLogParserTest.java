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
package org.araqne.logparser.syslog.infosec;

import static org.junit.Assert.assertEquals;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * @author kyun
 */
public class AntiWebShellLogParserTest {

	@Test
	public void testParser() {
		String line = "--__--infosec_-_syslog_confirm_66_-_JSP_-_https://10.251.8.62_-_10.251.8.62_-_2014-03-10 19:47:56_-_Y_-_N_-_shell_exec('_-_N_-_N_-_N_-_/usr/tomcat7/webapps/WSM/test/test_sh/Liz0ziM Private Safe Mode Command Execuriton Bypas1532_-_1532_-_Y_-_ _-_webshell_013_-_/usr/tomcat7/webapps/WSM/test/test_sh_-_/_-_Liz0ziM Private Safe Mode Command Execuriton Bypas9fbd6bd6dc90fbf4ac11e089c6e3c0db_-_9fbd6bd6dc90fbf4ac11e089c6e3c0db_-_hyung0925@skinfosec.co.kr_-_ _-_admin_-_webshell_skplanet@sk.com_-_010-0000-0000_-_0200000000_-_N_-_0_-_0_-_127.0.0.1";

		AntiWebShellLogParser parser = new AntiWebShellLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("--__--infosec", m.get("그룹명"));
		assertEquals("syslog_confirm_66", m.get("hostname"));
		assertEquals("JSP", m.get("type"));
		assertEquals("https://10.251.8.62", m.get("domain"));
		assertEquals("10.251.8.62", m.get("ip주소"));
		Date time = (Date) m.get("점검일");
		Calendar c = Calendar.getInstance();
		c.setTime(time);
		assertEquals(2014, c.get(Calendar.YEAR));
		assertEquals(2, c.get(Calendar.MONTH));
		assertEquals(10, c.get(Calendar.DAY_OF_MONTH));
		assertEquals(19, c.get(Calendar.HOUR_OF_DAY));
		assertEquals(47, c.get(Calendar.MINUTE));
		assertEquals(56, c.get(Calendar.SECOND));
		assertEquals("Y", m.get("생성"));
		assertEquals("N", m.get("변경"));
		assertEquals("shell_exec('", m.get("탐지문자"));
		assertEquals("N", m.get("권한변경"));
		assertEquals("N", m.get("이상탐지"));
		assertEquals("N", m.get("난독화"));
		assertEquals("/usr/tomcat7/webapps/WSM/test/test_sh/Liz0ziM Private Safe Mode Command Execuriton Bypas1532", m.get("위치"));
		assertEquals("1532", m.get("탐지테이블pk"));
		assertEquals("Y", m.get("웹쉘탐지상태"));
		assertEquals(null, m.get("조치상태"));
		assertEquals("webshell_013", m.get("진단명"));
		assertEquals("/usr/tomcat7/webapps/WSM/test/test_sh", m.get("검색시작디렉토리"));
		assertEquals("/", m.get("탐지디렉토리"));
		assertEquals("Liz0ziM Private Safe Mode Command Execuriton Bypas9fbd6bd6dc90fbf4ac11e089c6e3c0db", m.get("탐지파일명"));
		assertEquals("9fbd6bd6dc90fbf4ac11e089c6e3c0db", m.get("탐지해쉬값"));
		assertEquals("hyung0925@skinfosec.co.kr", m.get("담당자이메일주소"));
		assertEquals(null, m.get("담당자핸드폰번호"));
		assertEquals("admin", m.get("관리자명"));
		assertEquals("webshell_skplanet@sk.com", m.get("관리자이메일주소"));
		assertEquals("010-0000-0000", m.get("관리자핸드폰번호"));
		assertEquals("0200000000", m.get("관리자유선전화번호"));
		assertEquals("N", m.get("자동차단파일여부"));
		assertEquals("0", m.get("문자알림시작시간"));
		assertEquals("0", m.get("문자알림종료시간"));
		assertEquals("127.0.0.1", m.get("로컬서버ip주소"));
	}

	
	
	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
