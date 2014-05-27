/*
 * Copyright 2014 Eediom Inc
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

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author kyun
 */
public class DbSaferLogParser extends V1LogParser {

	private final Logger slog = LoggerFactory.getLogger(DbSaferLogParser.class);

	private final int SESSION = 100; // 사용자가 DBMS에 접속정보
	private final int SESSION_OUT = 110; // 사용자가 DBMS에 접속종료 정보
	private final int QUERY = 200; // 사용자가 DBMS에서 사용한 쿼리 정보
	private final int QUERY_STRING = 210; // 쿼리 내용 정보
	private final int QUERY_RTN_STR = 230; // 쿼리 실행 결과 메시지 정보
	private final int QUERY_RTN = 220; // 쿼리 실행 결과 정보
	private final int MANAGERLOG = 900; // 매니저 동작 로그
	private final int SYSTEMLOG = 901; // 시스템 동작 로그

	private int[] field_length;
	private String[] field_name;

	@Override
	public Map<String, Object> parse(Map<String, Object> log) {
		String line = (String) log.get("line");

		if (line == null)
			return log;

		try {
			Map<String, Object> m = new HashMap<String, Object>();

			m.put("버전", line.substring(8, 10));
			m.put("레코드종류", line.substring(10, 13));

			switch (getRecordType(line)) {
			case SESSION:
				parseSession();
				break;
			case SESSION_OUT:
				parseSessionOut();
				break;
			case QUERY:
				parseQuery();
				break;
			case QUERY_STRING:
				parseQueryString();
				break;
			case QUERY_RTN_STR:
				parseQueryRtnStr();
				break;
			case QUERY_RTN:
				parseQueryRtn();
				break;
			case MANAGERLOG:
				parseMngLog();
				break;
			case SYSTEMLOG:
				parseSysLog();
				break;
			default:
				return log;
			}

			String s = line.substring(13);

			int start = 0;
			for (int i = 0; i < field_name.length; i++) {
				if (field_length[i] < 0) {
					field_length[i] *= -1;
					int tmpLen = Integer.parseInt(s.substring(start, start + field_length[i]));
					start += field_length[i];
					field_length[i] = tmpLen;
					String tmpVal;
					tmpVal = subByteString(s.substring(start), field_length[i]);//
					m.put(field_name[i], tmpVal);
					start += tmpVal.length();
				} else {
					m.put(field_name[i], s.substring(start, start + field_length[i]));
					start += field_length[i];
				}
			}

			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne syslog parser: pnp secure dbsafer parse error - [" + line + "]", t);
		
			return log;
		}
	}

	// SESSION
	private void parseSession() {
		int[] length = { 32, 14, 15, 15, 5, 10, 20, 1, 1, -4, -4, -4, -4, -4, -4, -4 };
		String[] keys = { "세션키", "로그인시간", "사용자ip", "dbms서버ip", "dbms서버port", "서비스번호", "정책번호", "허용여부", "alert유무", "alert등급",
				"dbms계정", "프로그램명", "os정보", "인증id", "인증사용자명", "인증기타정보" };

		this.field_length = length;
		this.field_name = keys;
	}

	// SESSION_OUT
	private void parseSessionOut() {
		int[] length = { 32, 14, 14 };//
		String[] keys = { "세션키", "로그인시간", "로그아웃시간" };
		this.field_length = length;
		this.field_name = keys;
	}

	// QUERY
	private void parseQuery() {
		int[] length = { 32, 14, 10, 20, 1, 1, -4 };
		String[] keys = { "세션키", "쿼리실행시간", "쿼리번호", "정책번호", "허용여부", "alert유무", "alert등급" };
		this.field_length = length;
		this.field_name = keys;
	}

	// QUERY_STRING
	private void parseQueryString() {
		int[] length = { 32, 14, 10, -5 };
		String[] keys = { "세션키", "쿼리실행시간", "쿼리번호", "쿼리" };
		this.field_length = length;
		this.field_name = keys;
	}

	// QUERY_RTN_STR
	private void parseQueryRtnStr() {
		int[] length = { 32, 14, 10, -5 };
		String[] keys = { "세션키", "쿼리실행시간", "쿼리번호", "결과메시지" };
		this.field_length = length;
		this.field_name = keys;
	}

	// QUERY_RTN
	private void parseQueryRtn() {
		int[] length = { 32, 14, 14, 10, 11, 11 };
		String[] keys = { "세션키", "쿼리실행시간", "쿼리종료시간", "쿼리번호", "수행시간", "응답크기" };
		this.field_length = length;
		this.field_name = keys;
	}

	// MANAGERLOG
	private void parseMngLog() {
		int[] length = { 32, 32, 14, 32, 15, -4, 1 };
		String[] keys = { "키", "세션키", "로그발생시간", "관리자계정", "접속ip", "내용설명", "성공실패여부" };
		this.field_length = length;
		this.field_name = keys;
	}

	// SYSTEMLOG
	private void parseSysLog() {
		int[] length = { 14, -4 };
		String[] keys = { "로그발생시간", "상세내용" };
		this.field_length = length;
		this.field_name = keys;
	}

	private int getRecordType(String s) {
		return Integer.parseInt(s.substring(10, 13));
	}

	private String subByteString(String str, int endIndex) throws UnsupportedEncodingException {
		if (endIndex < 1)
			return "";

		StringBuffer sbStr = new StringBuffer(endIndex);
		int tmpIndex = 0;
		while (endIndex > 0) {
			String tmpSub = str.substring(tmpIndex, tmpIndex + (endIndex + 1) / 2);
			tmpIndex = tmpIndex + (endIndex + 1) / 2;
			sbStr.append(tmpSub);
			int t = tmpSub.getBytes("EUC-KR").length;
			endIndex -= t;
		}
		return sbStr.toString();
	}

}
