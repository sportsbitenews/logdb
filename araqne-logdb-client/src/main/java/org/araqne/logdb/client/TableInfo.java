/*
 * Copyright 2013 Eediom Inc.
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

import java.util.HashMap;
import java.util.Map;

/**
 * 테이블 설정을 표현합니다.
 * 
 * @author xeraph@eediom.com
 * 
 */
public class TableInfo {
	private String name;

	private TableSchemaInfo schema = new TableSchemaInfo();
	private Map<String, String> metadata = new HashMap<String, String>();

	public TableInfo() {
	}

	public TableInfo(String name, Map<String, String> metadata) {
		this.name = name;
		this.metadata = metadata;
	}

	/**
	 * 테이블 이름을 반환합니다.
	 * 
	 * @return 테이블 이름
	 */
	public String getName() {
		return name;
	}

	/**
	 * 테이블 이름을 설정합니다.
	 * 
	 * @param name
	 *            테이블 이름
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * 테이블 스키마를 반환합니다.
	 * 
	 * @return 테이블 스키마
	 */
	public TableSchemaInfo getSchema() {
		return schema;
	}

	/**
	 * 테이블 스키마를 설정합니다.
	 * 
	 * @param schema
	 *            테이블 스키마
	 */
	public void setSchema(TableSchemaInfo schema) {
		this.schema = schema;
	}

	/**
	 * 테이블 메타데이터를 반환합니다.
	 * 
	 * @return 테이블 메타데이터
	 */
	public Map<String, String> getMetadata() {
		return metadata;
	}

	/**
	 * 테이블 메타데이터를 설정합니다.
	 * 
	 * @param metadata
	 *            테이블 메타데이터
	 */
	public void setMetadata(Map<String, String> metadata) {
		this.metadata = metadata;
	}

	@Override
	public String toString() {
		return "name=" + name + ", metadata=" + metadata;
	}
}
