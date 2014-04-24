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
package org.araqne.logparser.syslog.riorey;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.util.StringTokenizer;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kyun
 */
public class RioreySyslogParser extends V1LogParser {

	private final Logger slog = LoggerFactory.getLogger(RioreySyslogParser.class);

	@Override
	public Map<String, Object> parse(Map<String, Object> log) {
		String line = (String) log.get("line");
		if (line == null)
			return log;
		try {
			Map<String, Object> m = new HashMap<String, Object>();
			Stack<String> stk = new Stack<String>();
			int i = 0;
			int ptrK = -1;
			int ptrV = 0;

			line = parseType(m,  parseHD(m,line));
			while (i < line.length()) {
				char c = line.charAt(i);
				if (c == '<' ){
					ptrK = i + 1;
					if(ptrV < i){
						String v = line.substring(ptrV, i);
						String k = concatKey(stk);
						if (isInteger(v))
							m.put(k, Integer.valueOf(v));
						else
							m.put(k, v);
					}

				}else if (c == '>' && c != line.length()){
					ptrV = i + 1;
					if(ptrK < i){
						if(line.charAt(ptrK) != '/')
							stk.push(line.substring(ptrK, i ));
						else if(!stk.empty())
							stk.pop();
					}
				}
				i++;
			}
			return m;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne syslog parser :  rioresy sys parse error - [" + line + "]", t);
			return log;
		}
	}

	@SuppressWarnings("unchecked")
	private String concatKey(Stack<String> Stk){
		StringBuffer Key = new StringBuffer();
		Stack<String> tmpStk = (Stack<String>)Stk.clone();
		while(!tmpStk.empty())
			Key.insert(0,"_"+getName(tmpStk.pop()));
		return Key.toString().substring(1);
	}

	private String getName(String s){
		int i = s.indexOf("name=");

		if(i< 0 ) return s;
		else return s.substring(i+6,s.length()-1).toLowerCase();
	}

	private  String parseHD(Map<String, Object> m, String s) throws ParseException {

		int b = s.indexOf("<systemEntry ");
		int c = s.indexOf(">",b);

		String tmpStr = s.substring(b+13,c);

		StringTokenizer tok = new StringTokenizer(tmpStr, " ");
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss ");

		while (tok.hasMoreTokens()) {
			String token = tok.nextToken();
			int tmp = token.indexOf('=');
			String key = token.substring(0,tmp);
			String value = token.substring(tmp+2,token.length()-1);
			if(key.equals("timestamp"))
			{
				value = value.replace('T', ' ');
				value = value.replace('Z', ' ');
				m.put(key, format.parse(value));
			}	
			else 
				m.put(key, value);
		}

		return s.substring(s.indexOf('<',b+1));
	}

	private String parseType(Map<String, Object> m, String s){
		String tmpStr = s.substring(1,s.indexOf('>',1));
		m.put("type",tmpStr.toLowerCase());
		return s.substring(s.indexOf('>') + 1);
	}

	private boolean isInteger(String str) {
		char check;

		if (str.equals(""))
			return false;

		for (int i = 0; i < str.length(); i++) {
			check = str.charAt(i);
			if (check < 48 || check > 58) {// 해당 char값이 숫자가 아닐 경우
				if(check != 45) // 해당 char값이 -가 아닐 경우
					return false;
			}
		}
		return true;
	}
}
