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
package org.araqne.logstorage;

import java.util.List;
import java.util.Map;

import org.araqne.logstorage.file.LogFileReader;
import org.araqne.logstorage.file.LogFileWriter;
import org.araqne.storage.api.FilePath;

public interface LogFileService {
	String getType();

	/**
	 * @since 1.17.0
	 * @param f
	 *            .idx file path
	 * @return log count
	 */
	long count(FilePath f);

	LogFileWriter newWriter(Map<String, Object> options);

	LogFileReader newReader(String tableName, Map<String, Object> options);

	/**
	 * config specifications for table data file
	 * 
	 * @since add-storage-layer branch
	 * @return config specifications, at least empty list
	 */
	List<TableConfigSpec> getConfigSpecs();
	
	/**
	 * config specifications for table replica data file
	 * 
	 * @since add-storage-layer branch
	 * @return config specifications, at least empty list
	 */
	List<TableConfigSpec> getReplicaConfigSpecs();
	
	/**
	 * config specifications for table secondary data file
	 * 
	 * @since add-storage-layer branch
	 * @return config specifications, at least empty list
	 */
	List<TableConfigSpec> getSecondaryConfigSpecs();

	/**
	 * @return service specific global settings
	 */
	Map<String, String> getConfigs();

	void setConfig(String key, String value);

	void unsetConfig(String key);
}
