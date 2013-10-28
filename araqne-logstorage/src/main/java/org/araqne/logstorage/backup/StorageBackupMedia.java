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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @since 2.2.7
 * @author xeraph
 * 
 */
public interface StorageBackupMedia {
	Set<String> getTableNames() throws IOException;

	Map<String, String> getTableMetadata(String tableName) throws IOException;

	List<StorageMediaFile> getFiles(String tableName) throws IOException;

	InputStream getInputStream(String tableName, String fileName) throws IOException;

	long getFreeSpace() throws IOException;

	void copyFromMedia(StorageTransferRequest req) throws IOException;

	void copyToMedia(StorageTransferRequest req) throws IOException;

}
