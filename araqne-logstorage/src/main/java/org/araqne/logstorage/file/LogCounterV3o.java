/*
 * Copyright 2014 Eediom Inc.
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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogCounterV3o {
	private final Logger logger = LoggerFactory.getLogger(LogCounterV3o.class);

	public long getCount(FilePath idxFile) {
		if (idxFile.length() < 22) {
			logger.error("logpresso logstorage: cannot count, invalid .idx file (too short), file=" + idxFile.getAbsolutePath());
			return 0;
		}

		StorageInputStream is = null;
		short hdrSize = 0;
		try {
			is = idxFile.newInputStream();
			is.seek(20);
			hdrSize = is.readShort();
			if (hdrSize > 65536) {
				logger.error("logpresso logstorage: cannot count, too big header size " + hdrSize + ", file="
						+ idxFile.getAbsolutePath());
				return 0;
			}
		} catch (Throwable t) {
			logger.error("logpresso logstorage: cannot count, file=" + idxFile.getAbsolutePath(), t);
			return 0;
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
				}
			}
		}

		if (hdrSize == 0) {
			logger.error("logpresso logstorage: cannot count, invalid header size, file=" + idxFile.getAbsolutePath());
			return 0;
		}

		BufferedInputStream r = null;
		try {
			r = new BufferedInputStream(idxFile.newInputStream());
			r.skip(hdrSize);
			byte[] buf = new byte[28];

			int tot = 0;
			while (true) {
				int read = r.read(buf);
				if (read != 28)
					break;
				ByteBuffer b = ByteBuffer.wrap(buf);
				b.position(24);
				tot += b.getInt();
			}

			return tot;
		} catch (Throwable t) {
			logger.error("logpresso logstorage: cannot count .idx file " + idxFile.getAbsolutePath(), t);
			return 0;
		} finally {
			if (r != null) {
				try {
					r.close();
				} catch (IOException e) {
				}
			}
		}
	}
}
