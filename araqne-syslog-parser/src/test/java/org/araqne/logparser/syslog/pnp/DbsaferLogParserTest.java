/**
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
package org.araqne.logparser.syslog.pnp;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

/**
 * @author kyun
 */
public class DbsaferLogParserTest {

//	@Test
//	public void test(){
//		long start = System.currentTimeMillis();
//		
//		for(int i = 0; i < 1000000;i++)
//			testParserSysLog();
//		
//		long end =  System.currentTimeMillis() - start;
//		
//		System.out.println((1000 * 1000000  / end) + " " + end);
//			
//		
//	}
	@Test
	public void testParserSession() {
		String line = "FF000160011000801031908121921680020102970001 20080103190814192.168.2.10   192.168.2.128  015210000000001000000000000000000000000000005scott0004jdbc0014Java_TTC-8.2.0000000000000EE\n";
		DbsaferLogParser parser = new DbsaferLogParser();

		Map<String, Object> m = parser.parse(line(line));

		assertEquals("0801031908121921680020102970001 ", m.get("세션키"));
		assertEquals("20080103190814",m.get("로그인 시간"));
		assertEquals("192.168.2.10   ",m.get("사용자 IP"));
		assertEquals("192.168.2.128  ",m.get("DBMS 서버 IP"));
		assertEquals("01521", m.get("DBMS 서버 PORT"));
		assertEquals("0000000001", m.get("서비스 번호"));
		assertEquals("00000000000000000000", m.get("정책 번호"));
		assertEquals("0", m.get("허용 여부"));
		assertEquals("0", m.get("Alert 유무"));
		assertEquals("", m.get("Alert 등급"));
		assertEquals("scott", m.get("DBMS 계정"));
		assertEquals("jdbc", m.get("프로그램명"));
		assertEquals("Java_TTC-8.2.0", m.get("OS 정보"));
		assertEquals("", m.get("인증ID"));
		assertEquals("", m.get("인증사용자명"));
		assertEquals("", m.get("인증기타정보"));
	}

	@Test
	public void testParserSessionOut() {
		String line = "FF000068011100801031908121921680020102970001 2008010319081420080103190819EE\n";
		DbsaferLogParser parser = new DbsaferLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("0801031908121921680020102970001 ", m.get("세션키"));             
		assertEquals("20080103190814", m.get("로그인 시간"));
		assertEquals("20080103190819", m.get("로그아웃 시간"));

	}

	@Test
	public void testParserQuery() {
		String line = "FF000090012000801031908121921680020102970001 20080103190814000000000000000000000000000000000000EE\n"; 
		DbsaferLogParser parser = new DbsaferLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("0801031908121921680020102970001 ", m.get("세션키"));
		assertEquals("20080103190814", m.get("쿼리실행시간"));
		assertEquals("0000000000", m.get("쿼리번호"));
		assertEquals("00000000000000000000", m.get("정책 번호"));
		assertEquals("0", m.get("허용 여부"));
		assertEquals("0", m.get("Alert 유무"));
		assertEquals("", m.get("Alert 등급"));

	}

	@Test
	public void testParserQueryString() {
		String line = "FF000087012100801031908121921680020102970001 20080103190816000000000100018SELECT * FROM DUALEE\n";
		DbsaferLogParser parser = new DbsaferLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("0801031908121921680020102970001 ", m.get("세션키"));
		assertEquals("20080103190816", m.get("쿼리실행시간"));
		assertEquals("0000000001", m.get("쿼리번호"));
		assertEquals("SELECT * FROM DUAL", m.get("쿼리"));

	}

	@Test
	public void testParserQueryRtnStr() {
		String line = "FF000085012300801031908121921680020102970001 20080103190816000000000100016Return CompletedEE\n";
		DbsaferLogParser parser = new DbsaferLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("0801031908121921680020102970001 ", m.get("세션키"));
		assertEquals("20080103190816", m.get("쿼리실행시간"));
		assertEquals("0000000001", m.get("쿼리번호"));
		assertEquals("Return Completed", m.get("결과 메시지"));
	}
	
	@Test
	public void testParserQueryRtn() {
		String line = "FF000100012200801031908121921680020102970001 200801031908162008010319081600000000010000000000000000000080EE\n";
		DbsaferLogParser parser = new DbsaferLogParser();
		Map<String, Object> m = parser.parse(line(line));
		
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
		DbsaferLogParser parser = new DbsaferLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("1321342695262328010             ", m.get("세션키"));
		assertEquals("1321342695490896019             ", m.get("키"));
		assertEquals("20111115163815", m.get("로그 발생 시간"));
		assertEquals("admin                           ", m.get("관리자 계정"));
		assertEquals("192.168.3.17   ", m.get("접속 IP"));
		assertEquals("로그인. IP[192.168.3.17] ID[admin]", m.get("내용 설명"));
		assertEquals("1", m.get("성공 실패 여부"));
		
	}

	@Test
	public void testParserSysLog() {
		String line = "FF00006002901201111151637550034관리자 비밀번호를 초기화 했습니다.EE\n";
		DbsaferLogParser parser = new DbsaferLogParser();
		Map<String, Object> m = parser.parse(line(line));
	
		assertEquals("20111115163755", m.get("로그 발생 시간"));
		assertEquals("관리자 비밀번호를 초기화 했습니다.", m.get("상세 내용"));
		
	}
	
	

	

	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}

}
