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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public interface LogTableRegistry {
	boolean exists(String tableName);

	List<String> getTableNames();

	List<TableSchema> getTableSchemas();

	TableSchema getTableSchema(String tableName);

	TableSchema getTableSchema(String tableName, boolean required);

	void createTable(TableSchema schema);

	void alterTable(String tableName, TableSchema schema);

	void dropTable(String tableName);

	void addListener(TableEventListener listener);

	void removeListener(TableEventListener listener);

	Lock getExclusiveTableLock(String tableName, String owner);

	Lock getSharedTableLock(String tableName);

	LockStatus getTableLockStatus(String tableName);
}
