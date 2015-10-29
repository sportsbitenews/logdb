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

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.araqne.logstorage.file.LogFileWriter;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageManager;

public interface LogStorage {
	/**
	 * @return the storage directory
	 */
	FilePath getDirectory();

	/**
	 * @param tableName
	 *            the table name
	 * @return the directory path which contains table files
	 */
	FilePath getTableDirectory(String tableName);

	/**
	 * set storage directory
	 * 
	 * @param f
	 *            the storage directory path
	 */
	void setDirectory(FilePath f);

	LogStorageStatus getStatus();

	void start();

	void stop();

	void createTable(TableSchema schema);

	void ensureTable(TableSchema schema);

	void alterTable(String tableName, TableSchema schema);

	void dropTable(String tableName);

	LogRetentionPolicy getRetentionPolicy(String tableName);

	void setRetentionPolicy(LogRetentionPolicy policy);

	/**
	 * reload parameters
	 */
	void reload();

	void flush();

	Date getPurgeBaseline(String tableName);

	/**
	 * @since 1.18.0
	 */
	void purge(String tableName, Date day);

	void purge(String tableName, Date fromDay, Date toDay);

	Collection<Date> getLogDates(String tableName);

	Collection<Date> getLogDates(String tableName, Date from, Date to);

	void write(Log log) throws InterruptedException;

	void write(List<Log> logs) throws InterruptedException;

	Collection<Log> getLogs(String tableName, Date from, Date to, int limit);

	Collection<Log> getLogs(String tableName, Date from, Date to, long offset, int limit);

	CachedRandomSeeker openCachedRandomSeeker();

	LogCursor openCursor(String tableName, Date day, boolean ascending) throws IOException;

	void addLogListener(LogCallback callback);

	void removeLogListener(LogCallback callback);

	List<LogWriterStatus> getWriterStatuses();

	/**
	 * @since 2.7.0
	 */
	LogFileWriter getOnlineWriter(String tableName, Date day);

	/**
	 * @since 1.18.0
	 */
	void addEventListener(LogStorageEventListener listener);

	/**
	 * @since 1.18.0
	 */
	void removeEventListener(LogStorageEventListener listener);

	/**
	 * @since 2.8.17
	 */
	boolean search(TableScanRequest req) throws InterruptedException;

	/**
	 * @since 2.8.17
	 */
	boolean searchTablet(TableScanRequest req, Date day) throws InterruptedException;

	/*
	 * @since 2.5.3
	 */
	<T> void addEventListener(Class<T> clazz, T callback);

	<T> void removeEventListener(Class<T> clazz, T callback);

	/*
	 * @since 2.5.5
	 */
	UUID lock(LockKey storageLockKey, String purpose, long timeout, TimeUnit unit) throws InterruptedException;

	void unlock(LockKey storageLockKey, String purpose);

	void flush(String tableName);

	/**
	 * 
	 * @since 2.5.10-SNAPSHOT
	 */
	StorageManager getStorageManager();

	boolean tryWrite(Log log);

	boolean tryWrite(Log log, long timeout, TimeUnit unit) throws InterruptedException;

	boolean tryWrite(List<Log> log);

	boolean tryWrite(List<Log> log, long timeout, TimeUnit unit) throws InterruptedException;

	LockStatus lockStatus(LockKey storageLockKey);

	void purge(String tableName, Date day, boolean skipArgCheck);

	<T> void addFallback(Class<T> clazz, T fallback);

	<T> void removeFallback(Class<T> clazz, T fallback);

	long getDiskUsage(String tableName, Date from, Date to);

	long getDiskUsage(Set<String> tableNames, Date from, Date to);
}
