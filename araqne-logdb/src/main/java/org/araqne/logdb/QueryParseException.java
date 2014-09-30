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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryParseException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	private String type;
	private Integer offset;
	private String note;
	private Map<String, String> params;
	private int offsetS;
	private int offsetE;
	private List<Integer> offsetList;
	
	public QueryParseException(String type, int offset) {
		this(type, offset, null);
	}
	
	public QueryParseException(String type, int offset, String note) {
		this.type = type;
		this.offset = offset;
		this.note = note;
		
		this.offsetS = -1;
		this.offsetE = -1;
		this.params = null;
		this.offsetList = new ArrayList<Integer>();
	}

	/**
	 * 
	 * @since 2.4.25
	 * @author kyun
	 * 
	 */
	public QueryParseException(String type, int s, int e, Map<String, String> params){
		this.type = type;
		this.offsetS = s;
		this.offsetE = e;
		this.params = (params==null)? new HashMap<String, String>(): params;
		this.offsetList = new ArrayList<Integer>();
	}

	public void addOffset(int offset){
		offsetList.add(offset);
	}

	public String getType() {
		return type;
	}

	public Integer getOffset() {
		return offset;
	}

	public int getOffsetS(){
		return offsetS;
	}
	
	public int getOffsetE(){
		return offsetE;
	}
	
	public Map<String, String> getParams(){
		return params;
	}
	
	public List<Integer> getOffsetList(){
		return offsetList;
	}
	
	public boolean isDebugMode(){
		return true;
	}
	
	
	@Override
	public String getMessage() {
		String mark = "^";
		int start = offsetS;
		int end = offsetE;
		
		start +=6;
		end += 6;
		
		String s = (start >= end)?
				String.format("%" + (end +1) + "s", mark):
					String.format("%" +(start +1) + "s%" + (end - start) + "s", mark, mark); 
		s =   s + "\ntype=" + type + ", offset=" + offset + ", note=" + note  + ", offsets:" + offsetList.toString() + ", errorStart: " + offsetS + ", errorEnd: " + offsetE;
		
		return  params == null? s + "\n":  s + " ,param:" + params.toString() +"\n" ;
	}
}
