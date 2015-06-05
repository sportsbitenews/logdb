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
package org.araqne.logstorage.backup;

import java.io.File;

/**
 * @since 2.2.7
 * @author xeraph
 * 
 */
public class StorageFile {
	private String tableName;

	private int tableId;

	private long length;

	private boolean done;

	// save exception if io failed
	private Throwable exception;

	// storage or media file path
	private File file;

	public StorageFile(String tableName, int tableId, File file) {
		this.tableName = tableName;
		this.tableId = tableId;
		this.file = file;
		this.length = file.length();
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getTableId() {
		return tableId;
	}

	public void setTableId(int tableId) {
		this.tableId = tableId;
	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public boolean isDone() {
		return done;
	}

	public void setDone(boolean done) {
		this.done = done;
	}

	public Throwable getException() {
		return exception;
	}

	public void setException(Throwable exception) {
		this.exception = exception;
	}

	public String getFileName() {
		return file.getName();
	}

	public File getFile() {
		return file;
	}

	public void deleteFile(File mediaPath) {
		File f = new File(mediaPath, "table/" + tableId + "/" + file.getName());

		if (file.length() == f.length())
			file.delete();

	}
}
