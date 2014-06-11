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
package org.araqne.logstorage.file;

import java.io.IOException;

import org.araqne.storage.api.FilePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 1.17.2
 * @author xeraph
 *
 */
public class LogCounterV2 {
	public static final int INDEX_ITEM_SIZE = 4;

	private LogCounterV2() {
	}

	public static long count(FilePath idxFile) {
		Logger logger = LoggerFactory.getLogger(LogCounterV2.class);
		BufferedStorageInputStream is = null;
		try {
			is = new BufferedStorageInputStream(idxFile);

			LogFileHeader indexFileHeader = LogFileHeader.extractHeader(is);
			if (indexFileHeader.version() != 2) {
				logger.error("araqne logstorage: invalid v2 .idx file version, " + idxFile.getAbsolutePath());
				return 0;
			}

			long totalCount = 0;
			long length = is.length();
			long pos = indexFileHeader.size();

			while (pos < length) {
				is.seek(pos);
				long count = is.readInt();
				totalCount += count;
				pos += 4 + count * INDEX_ITEM_SIZE;
			}
			return totalCount;
		} catch (IOException e) {
			logger.error("araqne logstorage: cannot count v2 .idx file, " + idxFile.getAbsolutePath(), e);
			return 0;
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
				}
			}
		}
	}
}
