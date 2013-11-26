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
package org.araqne.logstorage.engine;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

// TODO : this class will be deprecated
public class DatapathUtil {
	private static File logDir;

	public static void setLogDir(File logDir) {
		DatapathUtil.logDir = logDir;
	}

	public static File getIndexFile(int tableId, Date day, String basePath) {
		File baseDir = logDir;
		if (basePath != null)
			baseDir = new File(basePath);

		String dateText = getDayText(day);
		File tableDir = new File(baseDir, Integer.toString(tableId));
		File datafile = new File(tableDir, dateText + ".idx");
		return datafile;
	}

	public static File getDataFile(int tableId, Date day, String basePath) {
		File baseDir = logDir;
		if (basePath != null)
			baseDir = new File(basePath);

		String dateText = getDayText(day);
		File tableDir = new File(baseDir, Integer.toString(tableId));
		File datafile = new File(tableDir, dateText + ".dat");
		return datafile;
	}

	public static File getKeyFile(int tableId, Date day, String basePath) {
		File baseDir = logDir;
		if (basePath != null)
			baseDir = new File(basePath);

		String dateText = getDayText(day);
		File tableDir = new File(baseDir, Integer.toString(tableId));
		File keyfile = new File(tableDir, dateText + ".key");
		return keyfile;
	}

	private static String getDayText(Date day) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String dateText = dateFormat.format(day);
		return dateText;
	}
}
