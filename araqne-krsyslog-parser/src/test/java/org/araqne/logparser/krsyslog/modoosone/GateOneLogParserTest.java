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
package org.araqne.logparser.krsyslog.modoosone;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.araqne.logparser.krsyslog.modoosone.GateOneLogParser;
import org.junit.Test;

/**
 * @author kyun
 */
public class GateOneLogParserTest {

	@Test
	public void testParser() {
		String line = "1395138428:#:20140318183912-1001240-10.202.211.82-172.19.112.55-4169.51345420:#:484:#:AACI:"
				+ "#:1001240:#:조진영:#:10.202.211.82:#:SKP:#::#::#:전체그룹:#:MVoIPvs1:#:172.19.112.55:#:mVOIP:#:"
				+ ":#:mVOIP:#:ssh:#:0:#:make"; 
				
		GateOneLogParser parser = new GateOneLogParser();

		Map<String, Object> m = parser.parse(line(line));
		
		
		assertEquals("1395138428", m.get("계정생성날짜"));
		assertEquals("20140318183912-1001240-10.202.211.82-172.19.112.55-4169.51345420", m.get("상세설명"));
		assertEquals("484", m.get("장비키코드"));
		assertEquals("AACI", m.get("최상위그룹코드"));
		assertEquals("1001240", m.get("서버계정"));
		
		assertEquals("조진영", m.get("사용자이름"));
		assertEquals("10.202.211.82", m.get("사용자ip"));
		assertEquals("SKP", m.get("업체명"));
		assertEquals("", m.get("부서"));
		assertEquals("", m.get("구분"));
		
		assertEquals("전체그룹", m.get("사용자그룹")); 
		assertEquals("MVoIPvs1", m.get("장비명"));
		assertEquals("172.19.112.55", m.get("장비ip"));
		assertEquals("mVOIP", m.get("부서2"));
		assertEquals("", m.get("팀"));
		
		assertEquals("mVOIP", m.get("장비그룹"));
		assertEquals("ssh", m.get("서비스명"));
		assertEquals("0", m.get("내부명령어구분코드"));
		assertEquals("make", m.get("명령어"));
			
	}

	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}

}
