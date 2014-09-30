/*
 * Copyright 2013 Future Systems
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
package org.araqne.logdb;

import java.util.HashMap;
import java.util.Map;

public class QueryParseInsideException extends RuntimeException {
	private static final long serialVersionUID = 6192675685428143600L;
	private String type;
	private Map<String, String> params;
	private int offsetS;
	private int offsetE;

	/**
	 * @since 2.4.25
	 * @author kyun
	 * 
	 */
	public QueryParseInsideException(String type, int s, int e, Map<String, String> params){
		this.type = type;
		this.offsetS = s;
		this.offsetE = e;
		//this.params = params;
		this.params = (params==null)? new HashMap<String, String>(): params;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Map<String, String> getParams() {
		return params;
	}

	public void setParams(Map<String, String> params) {
		this.params = params;
	}

	public int getOffsetS() {
		return offsetS;
	}

	public void setOffsetS(int rOffsetS) {
		this.offsetS = rOffsetS;
	}

	public int getOffsetE() {
		return offsetE;
	}

	public void setOffsetE(int rOffsetE) {
		this.offsetE = rOffsetE;
	}

	public boolean isDebugMode(){
		return true;
	}

	@Override
	public String getMessage() {
		String mark = "^";
		int start = offsetS;
		int end = offsetE;


		String s = "";
		if(start > 0 || end > 0){
			if(start >= end)
				s = String.format("%" + (end +1) + "s", mark);
			else 
				s =	String.format("%" +(start +1) + "s%" + (end - start) + "s", mark, mark); 
			s += "\n";
		}

		s = s  + "errorcode " + type;
		return  params == null? s :  s + ", param:" + params.toString();
	}
}
