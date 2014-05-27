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

import java.util.List;

/**
 * 테이블 스키마를 표현합니다.
 * 
 * @since 0.9.0
 * @author xeraph@eediom.com
 * 
 */
public class TableSchemaInfo {
	private List<FieldInfo> fieldDefinitions;

	public TableSchemaInfo() {
	}

	/**
	 * 스키마를 구성하는 필드 정의 목록을 반환합니다.
	 * 
	 * @return 필드 정의 목록
	 */
	public List<FieldInfo> getFieldDefinitions() {
		return fieldDefinitions;
	}

	/**
	 * 스키마를 구성하는 필드 정의 목록을 설정합니다.
	 * 
	 * @param fieldDefinitions
	 *            필드 정의 목록
	 */
	public void setFieldDefinitions(List<FieldInfo> fieldDefinitions) {
		this.fieldDefinitions = fieldDefinitions;
	}
}
