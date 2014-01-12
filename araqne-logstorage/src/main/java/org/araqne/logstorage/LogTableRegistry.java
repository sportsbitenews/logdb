/*
 * Copyright 2010 NCHOVY
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
package org.araqne.logstorage;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.araqne.log.api.FieldDefinition;

public interface LogTableRegistry {
	public static final String LogFileTypeKey = "_filetype";

	boolean exists(String tableName);

	Collection<String> getTableNames();

	int getTableId(String tableName);

	String getTableName(int tableId);

	void createTable(String tableName, String type, Map<String, String> tableMetadata);

	void renameTable(String currentName, String newName);

	void dropTable(String tableName);

	List<FieldDefinition> getTableFields(String tableName);

	/**
	 * update table field definitions
	 * 
	 * @param tableName
	 *            existing table name
	 * @param fields
	 *            field definitions or null
	 * @since 2.5.1
	 */
	void setTableFields(String tableName, List<FieldDefinition> fields);

	Set<String> getTableMetadataKeys(String tableName);

	String getTableMetadata(String tableName, String key);

	void setTableMetadata(String tableName, String key, String value);

	void unsetTableMetadata(String tableName, String key);

	void addListener(LogTableEventListener listener);

	void removeListener(LogTableEventListener listener);
}
