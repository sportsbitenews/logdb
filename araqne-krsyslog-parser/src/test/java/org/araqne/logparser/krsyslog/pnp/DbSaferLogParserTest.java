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
package org.araqne.logparser.krsyslog.pnp;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.araqne.logparser.krsyslog.pnp.DbSaferLogParser;
import org.junit.Test;

/**
 * @author kyun
 */
public class DbSaferLogParserTest {

	@Test
	public void testParserSession() {
		String line = "FF000160011000801031908121921680020102970001 20080103190814192.168.2.10   192.168.2.128  015210000000001000000000000000000000000000005scott0004jdbc0014Java_TTC-8.2.00004kyun0006김택현0007오류101EE\n";
		DbSaferLogParser parser = new DbSaferLogParser();

		Map<String, Object> m = parser.parse(line(line));

		assertEquals("01", m.get("버전"));
		assertEquals("100", m.get("레코드종류"));
		assertEquals("0801031908121921680020102970001 ", m.get("세션키"));
		assertEquals("20080103190814",m.get("로그인시간"));
		assertEquals("192.168.2.10   ",m.get("사용자ip"));
		assertEquals("192.168.2.128  ",m.get("dbms서버ip"));
		assertEquals("01521", m.get("dbms서버port"));
		assertEquals("0000000001", m.get("서비스번호"));
		assertEquals("00000000000000000000", m.get("정책번호"));
		assertEquals("0", m.get("허용여부"));
		assertEquals("0", m.get("alert유무"));
		assertEquals("", m.get("alert등급"));
		assertEquals("scott", m.get("dbms계정"));
		assertEquals("jdbc", m.get("프로그램명"));
		assertEquals("Java_TTC-8.2.0", m.get("os정보"));
		assertEquals("kyun", m.get("인증id"));
		assertEquals("김택현", m.get("인증사용자명"));
		assertEquals("오류101", m.get("인증기타정보"));
	}

	@Test
	public void testParserSessionOut() {
		String line = "FF000068011100801031908121921680020102970001 2008010319081420080103190819EE\n";
		DbSaferLogParser parser = new DbSaferLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("01", m.get("버전"));
		assertEquals("110", m.get("레코드종류"));
		assertEquals("0801031908121921680020102970001 ", m.get("세션키"));             
		assertEquals("20080103190814", m.get("로그인시간"));
		assertEquals("20080103190819", m.get("로그아웃시간"));

	}

	@Test
	public void testParserQuery() {
		String line = "FF000090012000801031908121921680020102970001 20080103190814000000000000000000000000000000000000EE\n"; 
		DbSaferLogParser parser = new DbSaferLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("01", m.get("버전"));
		assertEquals("200", m.get("레코드종류"));
		assertEquals("0801031908121921680020102970001 ", m.get("세션키"));
		assertEquals("20080103190814", m.get("쿼리실행시간"));
		assertEquals("0000000000", m.get("쿼리번호"));
		assertEquals("00000000000000000000", m.get("정책번호"));
		assertEquals("0", m.get("허용여부"));
		assertEquals("0", m.get("alert유무"));
		assertEquals("", m.get("alert등급"));

	}

	@Test
	public void testParserQueryString() {
		String line = "FF000087012100801031908121921680020102970001 20080103190816000000000100018SELECT * FROM DUALEE\n";
		DbSaferLogParser parser = new DbSaferLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("01", m.get("버전"));
		assertEquals("210", m.get("레코드종류"));
		assertEquals("0801031908121921680020102970001 ", m.get("세션키"));
		assertEquals("20080103190816", m.get("쿼리실행시간"));
		assertEquals("0000000001", m.get("쿼리번호"));
		assertEquals("SELECT * FROM DUAL", m.get("쿼리"));

	}

	@Test
	public void testParserQueryRtnStr() {
		String line = "FF000085012300801031908121921680020102970001 20080103190816000000000100016Return CompletedEE\n";
		DbSaferLogParser parser = new DbSaferLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("01", m.get("버전"));
		assertEquals("230", m.get("레코드종류"));
		assertEquals("0801031908121921680020102970001 ", m.get("세션키"));
		assertEquals("20080103190816", m.get("쿼리실행시간"));
		assertEquals("0000000001", m.get("쿼리번호"));
		assertEquals("Return Completed", m.get("결과메시지"));
	}
	
	@Test
	public void testParserQueryRtn() {
		String line = "FF000100012200801031908121921680020102970001 200801031908162008010319081600000000010000000000000000000080EE\n";
		DbSaferLogParser parser = new DbSaferLogParser();
		Map<String, Object> m = parser.parse(line(line));
		
		assertEquals("01", m.get("버전"));
		assertEquals("220", m.get("레코드종류"));
		assertEquals("0801031908121921680020102970001 ", m.get("세션키"));
		assertEquals("20080103190816", m.get("쿼리실행시간"));
		assertEquals("20080103190816", m.get("쿼리종료시간"));
		assertEquals("0000000001", m.get("쿼리번호"));
		assertEquals("00000000000", m.get("수행시간"));
		assertEquals("00000000080", m.get("응답크기"));
		
	}
	
	@Test
	public void testParserMngLog() {
		String line = "FF000172029001321342695490896019             1321342695262328010             20111115163815admin"
				+ "                           192.168.3.17   0034로그인. IP[192.168.3.17] ID[admin]1EE\n";
		DbSaferLogParser parser = new DbSaferLogParser();
		Map<String, Object> m = parser.parse(line(line));
		
		assertEquals("02", m.get("버전"));
		assertEquals("900", m.get("레코드종류"));
		assertEquals("1321342695490896019             ", m.get("키"));
		assertEquals("1321342695262328010             ", m.get("세션키"));
		assertEquals("20111115163815", m.get("로그발생시간"));
		assertEquals("admin                           ", m.get("관리자계정"));
		assertEquals("192.168.3.17   ", m.get("접속ip"));
		assertEquals("로그인. IP[192.168.3.17] ID[admin]", m.get("내용설명"));
		assertEquals("1", m.get("성공실패여부"));
		
	}

	@Test
	public void testParserSysLog() {
		String line = "FF00006002901201111151637550034관리자 비밀번호를 초기화 했습니다.EE\n";
		DbSaferLogParser parser = new DbSaferLogParser();
		Map<String, Object> m = parser.parse(line(line));
	
		assertEquals("02", m.get("버전"));
		assertEquals("901", m.get("레코드종류"));
		assertEquals("20111115163755", m.get("로그발생시간"));
		assertEquals("관리자 비밀번호를 초기화 했습니다.", m.get("상세내용"));
		
	}
	
	

	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}

}
