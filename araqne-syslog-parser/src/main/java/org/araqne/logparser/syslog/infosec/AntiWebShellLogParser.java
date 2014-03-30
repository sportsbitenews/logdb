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
package org.araqne.logparser.syslog.infosec;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 

	[샘플로그] 구분자 : _-_
	--__--인포섹_-_syslog_확인_66_-_JSP_-_https://10.251.8.62_-_10.251.8.62_-_2014-03-10 19:47:56_-_Y_-_N_-_shell_exec('_-_N_-_N_-_N_-_/usr/tomcat7/webapps/WSM/test/test_sh/Liz0ziM Private Safe Mode Command Execuriton Bypas1532_-_1532_-_Y_-_ _-_웹쉘_패턴_013_-_/usr/tomcat7/webapps/WSM/test/test_sh_-_/_-_Liz0ziM Private Safe Mode Command Execuriton Bypas9fbd6bd6dc90fbf4ac11e089c6e3c0db_-_9fbd6bd6dc90fbf4ac11e089c6e3c0db_-_hyung0925@skinfosec.co.kr_-_ _-_최고관리자_-_webshell_skplanet@sk.com_-_010-0000-0000_-_0200000000_-_N_-_0_-_0_-_127.0.0.1
	
	
	그룹명 : --__--인포섹
	HostName : syslog_확인_66
	Type : JSP
	Domain : https://10.251.8.62
	IP주소 : 10.251.8.62
	점검일 : 2014-03-10 19:47:56
	생성 : Y
	
	변경 : N
	탐지문자 : shell_exec('
	권한변경 : N
	이상탐지 : N
	난독화 : N
	위치 : /usr/tomcat7/webapps/WSM/test/test_sh/Liz0ziM Private Safe Mode Command Execuriton Bypas1532
	탐지테이블PK : 1532
	웹쉘탐지상태 : Y
	
	조치상태 : 
	진단명 : 웹쉘_패턴_013
	검색시작디렉토리 : /usr/tomcat7/webapps/WSM/test/test_sh
	탐지디렉토리 : /
	탐지파일명 : Liz0ziM Private Safe Mode Command Execuriton Bypas9fbd6bd6dc90fbf4ac11e089c6e3c0db
	탐지해쉬값 : 9fbd6bd6dc90fbf4ac11e089c6e3c0db
	담당자이메일주소 : hyung0925@skinfosec.co.kr
	담당자핸드폰번호 : 

	관리자명 : 최고관리자
	관리자이메일주소 : webshell_skplanet@sk.com
	관리자핸드폰번호 : 010-0000-0000_-_0200000000
	관리자유선전화번호 : 
	자동차단파일여부 : N
	문자알림시작시간 : 0
	문자알림종료시간: 0
	로컬서버IP주소 : 127.0.0.1
	

 * @author kyun
 * @since 1.4.0
 */
public class AntiWebShellLogParser extends V1LogParser {
	private final Logger slog = LoggerFactory.getLogger(AntiWebShellLogParser.class);

	private enum FieldType {
		String, Integer, Date
	};

	private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
	
	private static final String[] Keys = new String[] {
		"그룹명", 
		"hostname", 
		"type", 
		"domain", 
		"ip주소",
		"점검일",
		"생성",
		"변경",
		"탐지문자", 
		"권한변경",
		"이상탐지",
		"난독화",
		"위치",
		"탐지테이블pk", 
		"웹쉘탐지상태",
		"조치상태", 
		"진단명", 
		"검색시작디렉토리", 
		"탐지디렉토리", 
		"탐지파일명", 
		"탐지해쉬값", 
		"담당자이메일주소", 
		"담당자핸드폰번호", 
		"관리자명", 
		"관리자이메일주소", 
		"관리자핸드폰번호", 
		"관리자유선전화번호", 
		"자동차단파일여부", 
		"문자알림시작시간", 
		"문자알림종료시간",
		"로컬서버ip주소"}; 


	private static final FieldType[] Types = new FieldType[] { 
		FieldType.String, FieldType.String, FieldType.String, FieldType.String, FieldType.String,
		FieldType.Date, FieldType.String, FieldType.String, FieldType.String, FieldType.String,
		FieldType.String, FieldType.String, FieldType.String, FieldType.String, FieldType.String,
		FieldType.String, FieldType.String, FieldType.String, FieldType.String, FieldType.String,
		FieldType.String, FieldType.String, FieldType.String, FieldType.String, FieldType.String,
		FieldType.String, FieldType.String, FieldType.String, FieldType.String, FieldType.String,
		FieldType.String};
	
	@Override
	public Map<String, Object> parse(Map<String, Object> log) {

		String line = (String) log.get("line");
		if (line == null)
			return log;

		int i = 0;
		Map<String, Object> m = new HashMap<String, Object>();

	
		
		try {
			
			String[] tokens  =line.split("_-_");//,Keys.length);
		
			for(String s : tokens){
				if (i >=Keys.length)
					break;
					
				String key = Keys[i];
				FieldType type = Types[i++];
				String token = s.trim();
				if (!token.equals("")) {
					if (type == FieldType.Integer)
						m.put(key, Integer.valueOf(token));
					else if (type == FieldType.Date)
						m.put(key, format.parse(token));
					else
						m.put(key, token);
				
					}
			}

			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("logpresso Anti Web-shell: parse error - [{}]", line);
			return log;
		}
	}

}
