package org.araqne.logstorage.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogCounterV2 {
	public static final int INDEX_ITEM_SIZE = 4;

	private LogCounterV2() {
	}

	public static long count(File idxFile) {
		Logger logger = LoggerFactory.getLogger(LogCounterV2.class);
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(idxFile, "r");

			LogFileHeader indexFileHeader = LogFileHeader.extractHeader(raf, idxFile);
			if (indexFileHeader.version() != 2) {
				logger.error("araqne logstorage: invalid v2 .idx file version, " + idxFile.getAbsolutePath());
				return 0;
			}

			long totalCount = 0;
			long length = raf.length();
			long pos = indexFileHeader.size();

			while (pos < length) {
				raf.seek(pos);
				long count = raf.readInt();
				totalCount += count;
				pos += 4 + count * INDEX_ITEM_SIZE;
			}
			return totalCount;
		} catch (IOException e) {
			logger.error("araqne logstorage: cannot count v2 .idx file, " + idxFile.getAbsolutePath(), e);
			return 0;
		} finally {
			if (raf != null) {
				try {
					raf.close();
				} catch (IOException e) {
				}
			}
		}
	}
}
