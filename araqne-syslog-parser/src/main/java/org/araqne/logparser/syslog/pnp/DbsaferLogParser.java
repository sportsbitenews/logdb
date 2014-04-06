/**
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
public class DbsaferLogParser extends V1LogParser {

	private final Logger slog = LoggerFactory.getLogger(DbsaferLogParser.class);

	private final int SESSION = 100; //사용자가 DBMS에 접속정보 
	private final int SESSION_OUT = 110; //사용자가 DBMS에 접속종료 정보
	private final int QUERY = 200; //사용자가 DBMS에서 사용한 쿼리 정보
	private final int QUERY_STRING = 210; //	쿼리 내용 정보
	private final int QUERY_RTN_STR = 230; //	쿼리 실행 결과 메시지 정보
	private final int QUERY_RTN = 220; //	쿼리 실행 결과 정보
	private final int MANAGERLOG = 900; //	매니저 동작 로그
	private final int SYSTEMLOG = 901; //시스템 동작 로그

	@Override
	public Map<String, Object> parse(Map<String, Object> log) {
		String line = (String) log.get("line");

		if (line == null)
			return log;
		try {
			Map<String, Object> m = new HashMap<String, Object>();

			switch (getRecordType(line)) {
			case SESSION:
				parseSession(m,line.substring(13));
				break;
			case SESSION_OUT:
				parseSessionOut(m,line.substring(13));
				break;
			case QUERY:
				parseQuery(m,line.substring(13));
				break;
			case QUERY_STRING:
				parseQueryString(m,line.substring(13));
				break;
			case QUERY_RTN_STR:
				parseQueryRtnStr(m,line.substring(13));
				break;
			case QUERY_RTN:
				parseQueryRtn(m,line.substring(13));
				break;
			case MANAGERLOG:
				parseMngLog(m,line.substring(13));
				break;
			case SYSTEMLOG:
				parseSysLog(m,line.substring(13));
				break;
			default:
				return log;
			}

			return m;

		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne syslog parser : db safer parse error - [{}]", line);
			return log;
		}
	}

	private void parseLine(Map<String, Object> m , String s,int[] length, String[] keys ) throws UnsupportedEncodingException{
		int start = 0;
		for(int i = 0; i < keys.length; i++){
			if(length[i] < 0){	
				length[i] *= -1;
				int tmpLen = Integer.parseInt(s.substring(start, start + length[i]));
				start += length[i];
				length[i] = tmpLen;		
				
				String tmpVal = cutFirstStrInByte(s.substring(start),length[i]);
				m.put(keys[i], tmpVal);// cutFirstStrInByte(s.substring(start),length[i]));
				start += tmpVal.length();//cutFirstStrInByte(s.substring(start),length[i]).length();
			}
			
			else {
			m.put(keys[i], s.substring(start, start + length[i]));
			start += length[i];
			}
			
		}
	}

	
	private static String cutFirstStrInByte(String str, int endIndex) throws UnsupportedEncodingException
	{
		StringBuffer sbStr = new StringBuffer(endIndex);
		int iTotal=0;
		for(char c: str.toCharArray())
		{
			iTotal+=String.valueOf(c).getBytes("EUC-KR").length;
			if(iTotal>endIndex)
			{
				break;
			}
			sbStr.append(c);
		}
		return sbStr.toString();
	}
	
	
	//SESSION
	private void parseSession(Map<String, Object> m , String s) throws UnsupportedEncodingException{ 
		int []length = {32, 14, 15, 15, 5, 
				10, 20, 1, 1, -4,
				-4, -4, -4, -4, -4,
				-4};//, 2, 1};
		String[] keys = { "세션키", "로그인 시간", "사용자 IP",	"DBMS 서버 IP",	"DBMS 서버 PORT",
				"서비스 번호", "정책 번호",	"허용 여부", "Alert 유무", "Alert 등급", 
				"DBMS 계정", "프로그램명", "OS 정보", "인증ID", "인증사용자명",
		"인증기타정보"};//, "End Point", "LINE FEED" };
		parseLine(m,s,length,keys);
	}

	//SESSION_OUT
	private void parseSessionOut(Map<String, Object> m , String s) throws UnsupportedEncodingException{
		int []length = {32, 14, 14};//
		String[] keys = { "세션키", "로그인 시간", "로그아웃 시간"};
		parseLine(m,s,length,keys);
	}

	//QUERY
	private void parseQuery(Map<String, Object> m , String s) throws UnsupportedEncodingException{
		int []length = {32, 14, 10, 20, 1, 
				1, -4};
		String[] keys = { "세션키", "쿼리실행시간", "쿼리번호",	"정책 번호",	"허용 여부",
				"Alert 유무", "Alert 등급"};
		parseLine(m,s,length,keys);
	}

	//QUERY_STRING
	private void parseQueryString(Map<String, Object> m , String s) throws UnsupportedEncodingException{
		int []length = {32, 14, 10, -5};
		String[] keys = { "세션키", "쿼리실행시간", "쿼리번호",	"쿼리"};
		parseLine(m,s,length,keys);
	}

	//QUERY_RTN_STR
	private void parseQueryRtnStr(Map<String, Object> m , String s) throws UnsupportedEncodingException{
		int []length = {32, 14, 10, -5};
		String[] keys = { "세션키", "쿼리실행시간", "쿼리번호",	"결과 메시지"};
		parseLine(m,s,length,keys);
	}

	//QUERY_RTN
	private void parseQueryRtn(Map<String, Object> m , String s) throws UnsupportedEncodingException{
		int []length = {32, 14, 14, 10, 11, 11};
		String[] keys = { "세션키", "쿼리실행시간", "쿼리종료시간", "쿼리번호",	 "수행시간", "응답크기"};
		parseLine(m,s,length,keys);
	}

	//MANAGERLOG
	private void parseMngLog(Map<String, Object> m , String s) throws UnsupportedEncodingException{
		int []length = {32, 32, 14, 32, 15, -4, 1};
		String[] keys = {"키", "세션키", "로그 발생 시간", "관리자 계정", "접속 IP", "내용 설명", "성공 실패 여부"};
		parseLine(m,s,length,keys);
	}
	
	//SYSTEMLOG
	private void parseSysLog(Map<String, Object> m , String s) throws UnsupportedEncodingException{
		int []length = {14, -4};
		String[] keys = { "로그 발생 시간", "상세 내용"};
		parseLine(m,s,length,keys);
	}

	private int getRecordType(String s){
		return Integer.parseInt(s.substring(10,13));
	}

	
}
