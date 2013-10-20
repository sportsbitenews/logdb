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

import java.io.InputStream;

/**
 * @since 2.2.7
 * @author xeraph
 * 
 */
public class TransferRequest {
	// original table name
	private String tableName;

	private StorageFile storageFile;
	private MediaFile mediaFile;

	// use input stream and filename instead of storage input file
	private InputStream inputStream;
	private String mediaFileName;

	public TransferRequest(String tableName, InputStream is, String mediaFileName) {
		this.tableName = tableName;
		this.inputStream = is;
		this.mediaFileName = mediaFileName;
	}

	public TransferRequest(StorageFile storageFile, MediaFile mediaFile) {
		this.tableName = storageFile.getTableName();
		this.storageFile = storageFile;
		this.mediaFile = mediaFile;
	}

	public String getTableName() {
		return tableName;
	}

	public InputStream getInputStream() {
		return inputStream;
	}

	public StorageFile getStorageFile() {
		return storageFile;
	}

	public MediaFile getMediaFile() {
		return mediaFile;
	}

	public String getMediaFileName() {
		return mediaFileName;
	}
}
