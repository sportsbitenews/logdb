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
package org.araqne.logstorage.file;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.araqne.logstorage.DateUtil;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.localfile.LocalFilePath;

// TODO : this class will be deprecated. use table specified configuration
public class DatapathUtil {
	private static FilePath logDir;

	public static void setLogDir(File logDir) {
		setLogDir(new LocalFilePath(logDir));
	}
	
	public static void setLogDir(FilePath path) {
		DatapathUtil.logDir = path;
	}
	
	public static FilePath getIndexFile(int tableId, Date day, FilePath basePath) {
		if (basePath == null)
			basePath = logDir;
		
		String dateText = getDayText(day);
		FilePath tableDir = basePath.newFilePath(Integer.toString(tableId));
		FilePath datafile = tableDir.newFilePath(dateText + ".idx");
		return datafile;
	}

	public static FilePath getLocalIndexFile(int tableId, Date day, String basePath) {
		FilePath baseDir = logDir;
		if (basePath != null)
			baseDir = new LocalFilePath(basePath);
		
		return getIndexFile(tableId, day, baseDir);
	}

	public static FilePath getDataFile(int tableId, Date day, FilePath basePath) {
		if (basePath == null)
			basePath = logDir;

		String dateText = getDayText(day);
		FilePath tableDir = basePath.newFilePath(Integer.toString(tableId));
		FilePath datafile = tableDir.newFilePath(dateText + ".dat");
		return datafile;
	}

	public static FilePath getLocalDataFile(int tableId, Date day, String basePath) {
		FilePath baseDir = logDir;
		if (basePath != null)
			baseDir = new LocalFilePath(basePath);

		return getDataFile(tableId, day, baseDir);
	}

	public static FilePath getKeyFile(int tableId, Date day, FilePath basePath) {
		if (basePath == null)
			basePath = logDir;

		String dateText = getDayText(day);
		FilePath tableDir = basePath.newFilePath(Integer.toString(tableId));
		FilePath keyfile = tableDir.newFilePath(dateText + ".key");
		return keyfile;
	}

	public static FilePath getLocalKeyFile(int tableId, Date day, String basePath) {
		FilePath baseDir = logDir;
		if (basePath != null)
			baseDir = new LocalFilePath(basePath);
		
		return getKeyFile(tableId, day, baseDir);
	}

	private static String getDayText(Date day) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String dateText = dateFormat.format(DateUtil.getDay(day));
		return dateText;
	}
}
