package org.araqne.logstorage.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.araqne.logstorage.LogMarshaler;
import org.araqne.logstorage.file.IndexBlockV3Header;
import org.araqne.logstorage.file.LogRecord;
import org.araqne.logstorage.file.LogRecordCursor;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageUtil;
import org.araqne.storage.localfile.LocalFilePath;
import org.junit.Test;

public class LogFileReaderV3oTest {
	@Test
	public void skipReserveBlockTest() throws IOException {
		FilePath indexFile = new LocalFilePath("v3_test.idx");
		FilePath dataFile = new LocalFilePath("v3_test.dat");
		
		indexFile.delete();
		dataFile.delete();
		
		LogFileV3oTest.genLogFile(indexFile, dataFile);
		
		FilePath rsrvdIndexFile = new LocalFilePath("rsrvd.idx");
		FilePath rsrvdDataFile = new LocalFilePath("rsrvd.dat");

		rsrvdIndexFile.delete();
		rsrvdDataFile.delete();
		
		int firstDiff = 3;
		LinkedList<Integer> segmentsToCopy = new LinkedList<Integer>();
		for (int i = 0; i < firstDiff; ++i) {
			segmentsToCopy.add(i);
		}
		
		LogFileV3oTest.copyLogFileV3(indexFile, dataFile, rsrvdIndexFile, rsrvdDataFile, segmentsToCopy);

		LogFileV3o logfile1 = null;
		LogFileV3o logfile2 = null;
		try {
			logfile1 = new LogFileV3o(indexFile, dataFile);
			logfile2 = new LogFileV3o(rsrvdIndexFile, rsrvdDataFile);

			List<IndexBlockV3Header> f1Blocks = LogFileV3oTest.toList(logfile1.getIndexBlocks());
			List<IndexBlockV3Header> addedBlocks = f1Blocks.subList(firstDiff, f1Blocks.size());
			// reserve here
			logfile2.reserveBlocks(addedBlocks);

		} catch (Throwable t) {
			throw new IllegalStateException("skipReserveBlockTest fail");
		} finally {
			StorageUtil.ensureClose(logfile2);
			StorageUtil.ensureClose(logfile1);
		}
		
		List<String> wholeLogs = new ArrayList<String>();
		LogReaderConfigV3o readerConfig1 = new LogReaderConfigV3o();
		readerConfig1.checkIntegrity = false;
		readerConfig1.dataPath = dataFile;
		readerConfig1.indexPath = indexFile;
		readerConfig1.tableName = "lfwv3test";

		LogFileReaderV3o reader1 = null;
		try {
			reader1 = new LogFileReaderV3o(readerConfig1);
			LogRecordCursor cursor = reader1.getCursor();
			while (cursor.hasNext()) {
				LogRecord next = cursor.next();
				wholeLogs.add(LogMarshaler.convert("lfwv3test", next).toString());
			}
		} finally {
			if (reader1 != null)
				reader1.close();
		}
		
		List<String> skippedLogs = new ArrayList<String>();
		LogReaderConfigV3o readerConfig2 = new LogReaderConfigV3o();
		readerConfig2.checkIntegrity = false;
		readerConfig2.dataPath = rsrvdDataFile;
		readerConfig2.indexPath = rsrvdIndexFile;
		readerConfig2.tableName = "lfwv3test";

		LogFileReaderV3o reader2 = null;
		try {
			reader2 = new LogFileReaderV3o(readerConfig2);
			LogRecordCursor cursor = reader2.getCursor();
			while (cursor.hasNext()) {
				LogRecord next = cursor.next();
				skippedLogs.add(LogMarshaler.convert("lfwv3test", next).toString());
			}
		} finally {
			if (reader2 != null)
				reader2.close();
		}
		
		assertEquals(600, skippedLogs.size());
		assertArrayEquals(wholeLogs.subList(wholeLogs.size() - skippedLogs.size(), wholeLogs.size()).toArray(), 
				skippedLogs.toArray());
	
		rsrvdIndexFile.delete();
		rsrvdDataFile.delete();
		
		indexFile.delete();
		dataFile.delete();
	}
}
