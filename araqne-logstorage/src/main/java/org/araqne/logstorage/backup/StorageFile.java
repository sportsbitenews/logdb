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
public class StorageFile extends BaseFile {
	// storage or media file path
	private File file;

	public StorageFile(String tableName, File file) {
		this.tableName = tableName;
		this.file = file;
		this.length = file.length();
	}

	@Override
	public String getFileName() {
		return file.getName();
	}

	public File getFile() {
		return file;
	}
}
