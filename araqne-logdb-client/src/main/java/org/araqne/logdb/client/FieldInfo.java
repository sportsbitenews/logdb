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
package org.araqne.logdb.client;

/**
 * 테이블 스키마를 구성하는 개별 필드의 정보를 표현합니다.
 * 
 * @author xeraph@eediom.com
 * 
 */
public class FieldInfo {
	private String name;
	private String type;

	// 0 for unknown
	private int length;

	public FieldInfo() {
	}

	public FieldInfo(String name, String type) {
		this(name, type, 0);
	}

	public FieldInfo(String name, String type, int length) {
		this.name = name;
		this.type = type;
		this.length = length;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	@Override
	public String toString() {
		if (length > 0)
			return name + " " + type + "(" + length + ")";
		return name + " " + type;
	}
}
