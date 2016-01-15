/*
 * Copyright 2014 Eediom Inc. All rights reserved.
 */
package org.araqne.logstorage.file;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.araqne.logstorage.Log;
import org.araqne.logstorage.file.IndexBlockV3Header;
import org.araqne.logstorage.file.InvalidLogFileHeaderException;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageOutputStream;
import org.araqne.storage.api.StorageUtil;
import org.araqne.storage.filepair.CannotAppendBlockException;
import org.araqne.storage.filepair.CloseableEnumeration;
import org.araqne.storage.localfile.LocalFilePath;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

public class LogFileV3oTest {
	static Logger logger = LoggerFactory.getLogger(LogFileV3oTest.class.getSimpleName());

	private static FilePath indexFile;
	private static FilePath dataFile;
	private static FilePath dupIndexFile;
	private static FilePath dupDataFile;
	private static FilePath subIndexFile;
	private static FilePath subDataFile;
	private static FilePath rsrvdIndexFile;
	private static FilePath rsrvdDataFile;
	private static FilePath holeIndexFile;
	private static FilePath holeDataFile;
	private static FilePath addedIndexFile;
	private static FilePath addedDataFile;
	private static FilePath addedDupIndexFile;
	private static FilePath addedDupDataFile;
	private static FilePath emptyIndexFile;
	private static FilePath emptyDataFile;
	private static FilePath skipIndexFile;
	private static FilePath skipDataFile;
	private static FilePath truncIndexFile;
	private static FilePath truncDataFile;
	private static FilePath truncTargetIndexFile;
	private static FilePath truncTargetDataFile;

	@org.junit.BeforeClass
	public static void setup() throws IOException {
		indexFile = new LocalFilePath(".v3_test/v3_test.idx");
		dataFile = new LocalFilePath(".v3_test/v3_test.dat");
		dupIndexFile = new LocalFilePath(".v3_test/v3_test_dup.idx");
		dupDataFile = new LocalFilePath(".v3_test/v3_test_dup.dat");
		subIndexFile = new LocalFilePath(".v3_test/v3_test_sub.idx");
		subDataFile = new LocalFilePath(".v3_test/v3_test_sub.dat");
		rsrvdIndexFile = new LocalFilePath(".v3_test/v3_test_rsrvd.idx");
		rsrvdDataFile = new LocalFilePath(".v3_test/v3_test_rsrvd.dat");
		holeIndexFile = new LocalFilePath(".v3_test/v3_test_hole.idx");
		holeDataFile = new LocalFilePath(".v3_test/v3_test_hole.dat");
		addedIndexFile = new LocalFilePath(".v3_test/v3_test_added.idx");
		addedDataFile = new LocalFilePath(".v3_test/v3_test_added.dat");
		addedDupIndexFile = new LocalFilePath(".v3_test/v3_test_added_dup.idx");
		addedDupDataFile = new LocalFilePath(".v3_test/v3_test_added_dup.dat");
		emptyIndexFile = new LocalFilePath(".v3_test/v3_test_empty.idx");
		emptyDataFile = new LocalFilePath(".v3_test/v3_test_empty.dat");
		skipIndexFile = new LocalFilePath(".v3_test/v3_test_skip.idx");
		skipDataFile = new LocalFilePath(".v3_test/v3_test_skip.dat");
		truncIndexFile = new LocalFilePath(".v3_test/v3_test_trunc.idx");
		truncDataFile = new LocalFilePath(".v3_test/v3_test_trunc.dat");
		truncTargetIndexFile = new LocalFilePath(".v3_test/v3_test_trunc_target.idx");
		truncTargetDataFile = new LocalFilePath(".v3_test/v3_test_trunc_target.dat");

		logger.info("setup test");
		new LocalFilePath(".v3_test").mkdirs();
		indexFile.delete();
		dataFile.delete();
		dupIndexFile.delete();
		dupDataFile.delete();
		subIndexFile.delete();
		subDataFile.delete();
		rsrvdIndexFile.delete();
		rsrvdDataFile.delete();
		holeIndexFile.delete();
		holeDataFile.delete();
		addedIndexFile.delete();
		addedDataFile.delete();
		addedDupIndexFile.delete();
		addedDupDataFile.delete();
		emptyIndexFile.delete();
		emptyDataFile.delete();
		skipIndexFile.delete();
		skipDataFile.delete();
		truncIndexFile.delete();
		truncDataFile.delete();
		truncTargetIndexFile.delete();
		truncTargetDataFile.delete();

		genLogFile(indexFile, dataFile);

		copyLogFileV3(indexFile, dataFile, dupIndexFile, dupDataFile,
				new LinkedList<Integer>(Arrays.asList(0, 1, 2, 3, 4)));
		copyLogFileV3(indexFile, dataFile, subIndexFile, subDataFile,
				new LinkedList<Integer>(Arrays.asList(0, 1, 2)));
		copyLogFileV3(indexFile, dataFile, rsrvdIndexFile, rsrvdDataFile,
				new LinkedList<Integer>(Arrays.asList(0, 1, 2)));
		copyLogFileV3(indexFile, dataFile, holeIndexFile, holeDataFile,
				new LinkedList<Integer>(Arrays.asList(0, 1, 3)));
		copyLogFileV3(indexFile, dataFile, addedIndexFile, addedDataFile,
				new LinkedList<Integer>(Arrays.asList(0, 1, 2)));
		copyLogFileV3(indexFile, dataFile, addedDupIndexFile, addedDupDataFile,
				new LinkedList<Integer>(Arrays.asList(0, 1, 2)));
		copyLogFileV3(indexFile, dataFile, skipIndexFile, skipDataFile,
				new LinkedList<Integer>(Arrays.asList(0, 1, 2)));
		copyLogFileV3(indexFile, dataFile, truncIndexFile, truncDataFile,
				new LinkedList<Integer>(Arrays.asList(0, 1, 2, 3, 4)));
		copyLogFileV3(indexFile, dataFile, truncTargetIndexFile, truncTargetDataFile,
				new LinkedList<Integer>(Arrays.asList(0, 1)));

		logger.info("setup done.");
	}

	// @org.junit.AfterClass
	public void teardown() {
		indexFile.delete();
		dataFile.delete();
		rsrvdIndexFile.delete();
		rsrvdDataFile.delete();
		holeIndexFile.delete();
		holeDataFile.delete();
	}

	public static <T> List<T> toList(CloseableEnumeration<T> ce) {
		try {
			return Collections.list(ce);
		} finally {
			if (ce != null)
				StorageUtil.ensureClose(ce);
		}
	}

	@Test
	public void indexBlockTest() throws IOException {
		LogFileV3o logfile = null;
		try {
			logfile = new LogFileV3o(indexFile, dataFile);

			long indexSize = logfile.getIndexBlockCount();

			System.out.println("index block count: " + indexSize);
			List<IndexBlockV3Header> indexBlocks = toList(logfile.getIndexBlocks());
			for (IndexBlockV3Header indexBlock : indexBlocks) {
				System.out.println(indexBlock);
			}

			logfile.close();
		} finally {
			StorageUtil.ensureClose(logfile);
		}
	}

	@Test
	public void blockHashTest() throws IOException {
		LogFileV3o logfile = null;
		try {
			logfile = new LogFileV3o(indexFile, dataFile);

			long indexSize = logfile.getIndexBlockCount();

			System.out.println("index block count: " + indexSize);
			List<IndexBlockV3Header> indexBlocks = toList(logfile.getIndexBlocks());
			for (IndexBlockV3Header indexBlock : indexBlocks) {
				LogFileV3o.RawDataBlock rawBlock = logfile.getRawDataBlock(indexBlock);
				System.out.printf("blockId: %d, raw digest: %s\n", indexBlock.getId(), rawBlock.getDigest());
			}
		} finally {
			StorageUtil.ensureClose(logfile);
		}
	}

	@Test
	public void rewriteTest() throws IOException {
		LogFileV3o logfile = null;
		LogFileV3o logfile2 = null;
		try {
			logfile = new LogFileV3o(indexFile, dataFile);
			logfile2 = new LogFileV3o(dupIndexFile, dupDataFile);

			List<IndexBlockV3Header> indexBlocks = toList(logfile.getIndexBlocks());
			List<IndexBlockV3Header> indexBlocks2 = toList(logfile2.getIndexBlocks());

			for (int i = 0; i < indexBlocks.size(); ++i) {
				IndexBlockV3Header ib1 = indexBlocks.get(i);
				IndexBlockV3Header ib2 = indexBlocks2.get(i);

				assertTrue(ib1.equals(ib2));

				assertTrue(logfile.getRawDataBlock(ib1).getDigest().equals(logfile2.getRawDataBlock(ib2).getDigest()));
			}

		} catch (Throwable t) {
			throw new IllegalStateException("rewriteTest fail");
		} finally {
			StorageUtil.ensureClose(logfile2);
			StorageUtil.ensureClose(logfile);
		}
	}

	@Test
	public void getAddedBlockTest() throws IOException {
		LogFileV3o logfile = null;
		LogFileV3o logfile2 = null;
		try {
			logfile = new LogFileV3o(indexFile, dataFile);
			logfile2 = new LogFileV3o(subIndexFile, subDataFile);

			List<IndexBlockV3Header> srcBlocks = toList(logfile.getIndexBlocks());
			List<IndexBlockV3Header> dstBlocks = toList(logfile2.getIndexBlocks());

			int firstDiffBlock = getFirstDiffBlock(srcBlocks, dstBlocks);

			assertEquals(3, firstDiffBlock);

		} catch (Throwable t) {
			throw new IllegalStateException("getAddedBlockTest fail");
		} finally {
			StorageUtil.ensureClose(logfile2);
			StorageUtil.ensureClose(logfile);
		}
	}

	@Test
	public void getReservedBlockTest() throws IOException {
		LogFileV3o srclogfile = null;
		LogFileV3o logfile = null;
		try {
			srclogfile = new LogFileV3o(indexFile, dataFile);
			logfile = new LogFileV3o(holeIndexFile, holeDataFile);

			List<IndexBlockV3Header> srcBlocks = toList(srclogfile.getIndexBlocks());
			List<IndexBlockV3Header> testBlocks = toList(logfile.getIndexBlocks());

			logfile.reserveBlocks(srcBlocks.subList(4, srcBlocks.size()));

			boolean[] reserved = { false, false, true, false, true };

			int i = 0;
			for (IndexBlockV3Header block : testBlocks) {
				assertEquals(reserved[i++], block.isReserved());
			}

		} catch (Throwable t) {
			throw new IllegalStateException("getReservedBlockTest fail");
		} finally {
			StorageUtil.ensureClose(srclogfile);
			StorageUtil.ensureClose(logfile);
		}
	}

	@Test
	public void reserveAndReplaceBlockTest() throws IOException {
		LogFileV3o filter1 = null;
		LogFileV3o filter2 = null;
		try {
			filter1 = new LogFileV3o(indexFile, dataFile);
			filter2 = new LogFileV3o(rsrvdIndexFile, rsrvdDataFile);

			List<IndexBlockV3Header> f1Blocks = toList(filter1.getIndexBlocks());
			List<IndexBlockV3Header> f2Blocks = toList(filter2.getIndexBlocks());

			int firstDiff = getFirstDiffBlock(f1Blocks, f2Blocks);
			assertEquals(firstDiff, 3);

			List<IndexBlockV3Header> addedBlocks = f1Blocks.subList(firstDiff, f1Blocks.size());
			// reserve here
			filter2.reserveBlocks(addedBlocks);

			// length check
			assertEquals(indexFile.length(), rsrvdIndexFile.length());
			assertEquals(dataFile.length(), rsrvdDataFile.length());

			// test block reserved
			int i = 0;
			f2Blocks = toList(filter2.getIndexBlocks());
			for (IndexBlockV3Header block : f2Blocks) {
				assertEquals(i, block.getId());
				assertEquals(i >= firstDiff, block.isReserved());
				i++;
			}
			assertEquals(5, i);

			// replace blocks
			for (IndexBlockV3Header block : f1Blocks.subList(firstDiff, f1Blocks.size())) {
				filter2.replaceBlock(block, filter1.getRawDataBlock(block));
			}

			// length check
			assertEquals(indexFile.length(), rsrvdIndexFile.length());
			assertEquals(dataFile.length(), rsrvdDataFile.length());

			// test all block digests are same
			List<IndexBlockV3Header> newDstBlocks = toList(filter2.getIndexBlocks());

			for (int j = 0; j < f1Blocks.size(); ++j) {
				String digest = filter1.getRawDataBlock(f1Blocks.get(j)).getDigest();
				String digest2 = filter2.getRawDataBlock(newDstBlocks.get(j)).getDigest();

				assertEquals(digest, digest2);
			}
		} catch (Throwable t) {
			throw new IllegalStateException("reserveAndReplaceBlockTest fail");
		} finally {
			StorageUtil.ensureClose(filter2);
			StorageUtil.ensureClose(filter1);
		}
	}

	@Test
	public void reserveAndReplaceBlockEmptyFileTest() throws IOException {
		LogFileV3o filter1 = null;
		LogFileV3o filter2 = null;
		try {
			emptyIndexFile.delete();
			emptyDataFile.delete();

			filter1 = new LogFileV3o(indexFile, dataFile);
			filter2 = new LogFileV3o(emptyIndexFile, emptyDataFile);

			List<IndexBlockV3Header> f1Blocks = toList(filter1.getIndexBlocks());
			List<IndexBlockV3Header> f2Blocks = toList(filter2.getIndexBlocks());

			int firstDiff = getFirstDiffBlock(f1Blocks, f2Blocks);
			assertEquals(0, firstDiff);

			List<IndexBlockV3Header> addedBlocks = f1Blocks.subList(firstDiff, f1Blocks.size());
			assertEquals(f1Blocks, addedBlocks);

			// reserve here
			filter2.reserveBlocks(addedBlocks);

			// length check
			assertEquals(indexFile.length(), emptyIndexFile.length());
			assertEquals(dataFile.length(), emptyDataFile.length());

			// test block reserved
			int i = 0;
			f2Blocks = toList(filter2.getIndexBlocks());
			for (IndexBlockV3Header block : f2Blocks) {
				assertEquals(i, block.getId());
				assertEquals(i >= firstDiff, block.isReserved());
				i++;
			}
			assertEquals(5, i);

			// replace blocks
			for (IndexBlockV3Header block : f1Blocks.subList(firstDiff, f1Blocks.size())) {
				filter2.replaceBlock(block, filter1.getRawDataBlock(block));
			}

			// length check
			assertEquals(indexFile.length(), emptyIndexFile.length());
			assertEquals(dataFile.length(), emptyDataFile.length());

			// test all block digests are same
			List<IndexBlockV3Header> newDstBlocks = toList(filter2.getIndexBlocks());

			for (int j = 0; j < f1Blocks.size(); ++j) {
				String digest = filter1.getRawDataBlock(f1Blocks.get(j)).getDigest();
				String digest2 = filter2.getRawDataBlock(newDstBlocks.get(j)).getDigest();

				assertEquals(digest, digest2);
			}
		} catch (Throwable t) {
			throw new IllegalStateException("reserveAndReplaceBlockEmptyFileTest fail");
		} finally {
			StorageUtil.ensureClose(filter2);
			StorageUtil.ensureClose(filter1);
		}
	}

	@Test
	public void addBlockTest() throws IOException {
		LogFileV3o logfile1 = null;
		LogFileV3o logfile2 = null;
		try {
			logfile1 = new LogFileV3o(indexFile, dataFile);
			logfile2 = new LogFileV3o(addedIndexFile, addedDataFile);

			List<IndexBlockV3Header> f1Blocks = toList(logfile1.getIndexBlocks());
			List<IndexBlockV3Header> f2Blocks = toList(logfile2.getIndexBlocks());

			int firstDiff = getFirstDiffBlock(f1Blocks, f2Blocks);
			assertEquals(firstDiff, 3);

			// add blocks
			for (IndexBlockV3Header block : f1Blocks.subList(firstDiff, f1Blocks.size())) {
				logfile2.addBlock(block, logfile1.getRawDataBlock(block));
			}

			// length check
			assertEquals(indexFile.length(), addedIndexFile.length());
			assertEquals(dataFile.length(), addedDataFile.length());

			// test all block digests are same
			List<IndexBlockV3Header> newDstBlocks = toList(logfile2.getIndexBlocks());

			for (int j = 0; j < f1Blocks.size(); ++j) {
				String digest = logfile1.getRawDataBlock(f1Blocks.get(j)).getDigest();
				String digest2 = logfile2.getRawDataBlock(newDstBlocks.get(j)).getDigest();

				assertEquals(digest, digest2);
			}
		} finally {
			StorageUtil.ensureClose(logfile2);
			StorageUtil.ensureClose(logfile1);
		}
	}

	@Test(expected = CannotAppendBlockException.class)
	public void addDupBlockTest() throws IOException {
		LogFileV3o logfile1 = null;
		LogFileV3o logfile2 = null;
		try {
			logfile1 = new LogFileV3o(indexFile, dataFile);
			logfile2 = new LogFileV3o(addedDupIndexFile, addedDupDataFile);

			List<IndexBlockV3Header> f1Blocks = toList(logfile1.getIndexBlocks());
			List<IndexBlockV3Header> f2Blocks = toList(logfile2.getIndexBlocks());

			int firstDiff = getFirstDiffBlock(f1Blocks, f2Blocks);
			assertEquals(firstDiff, 3);

			// add blocks
			for (IndexBlockV3Header block : f1Blocks.subList(firstDiff, f1Blocks.size())) {
				logfile2.addBlock(block, logfile1.getRawDataBlock(block));
			}

			// length check
			assertEquals(indexFile.length(), addedDupIndexFile.length());
			assertEquals(dataFile.length(), addedDupDataFile.length());

			// test all block digests are same
			List<IndexBlockV3Header> newDstBlocks = toList(logfile2.getIndexBlocks());

			for (int j = 0; j < f1Blocks.size(); ++j) {
				String digest = logfile1.getRawDataBlock(f1Blocks.get(j)).getDigest();
				String digest2 = logfile2.getRawDataBlock(newDstBlocks.get(j)).getDigest();

				assertEquals(digest, digest2);
			}

			// add duplicated block test (throws exception)
			IndexBlockV3Header dupBlock = f1Blocks.get(f1Blocks.size() - 1);
			logfile2.addBlock(dupBlock, logfile1.getRawDataBlock(dupBlock));

		} finally {
			StorageUtil.ensureClose(logfile2);
			StorageUtil.ensureClose(logfile1);
		}
	}

	@Test(expected = CannotAppendBlockException.class)
	public void addSkippedBlockToEmptyFileTest() throws IOException {
		LogFileV3o logfile1 = null;
		LogFileV3o logfile2 = null;
		try {
			emptyIndexFile.delete();
			emptyDataFile.delete();

			logfile1 = new LogFileV3o(indexFile, dataFile);
			logfile2 = new LogFileV3o(emptyIndexFile, emptyDataFile);

			// add block
			IndexBlockV3Header block = logfile1.getIndexBlock(1);
			logfile2.addBlock(block, logfile1.getRawDataBlock(block));
		} finally {
			StorageUtil.ensureClose(logfile2);
			StorageUtil.ensureClose(logfile1);
		}

	}

	@Test(expected = CannotAppendBlockException.class)
	public void addSkippedBlockTest() throws IOException {
		LogFileV3o logfile1 = null;
		LogFileV3o logfile2 = null;
		try {
			logfile1 = new LogFileV3o(indexFile, dataFile);
			logfile2 = new LogFileV3o(skipIndexFile, skipDataFile);

			// add block
			IndexBlockV3Header block = logfile1.getIndexBlock(4);
			logfile2.addBlock(block, logfile1.getRawDataBlock(block));
		} finally {
			StorageUtil.ensureClose(logfile2);
			StorageUtil.ensureClose(logfile1);
		}

	}

	@Test
	public void addBlockToEmptyFileTest() throws IOException {
		LogFileV3o logfile1 = null;
		LogFileV3o logfile2 = null;
		try {
			emptyIndexFile.delete();
			emptyDataFile.delete();

			logfile1 = new LogFileV3o(indexFile, dataFile);
			logfile2 = new LogFileV3o(emptyIndexFile, emptyDataFile);
			logfile2.setIndexFileHeader(logfile1.getIndexFileHeader());
			logfile2.setDataFileHeader(logfile1.getDataFileHeader());

			List<IndexBlockV3Header> f1Blocks = toList(logfile1.getIndexBlocks());
			List<IndexBlockV3Header> f2Blocks = toList(logfile2.getIndexBlocks());

			int firstDiff = getFirstDiffBlock(f1Blocks, f2Blocks);
			assertEquals(firstDiff, 0);

			// add blocks
			for (IndexBlockV3Header block : f1Blocks.subList(firstDiff, f1Blocks.size())) {
				logfile2.addBlock(block, logfile1.getRawDataBlock(block));
			}

			// test all block digests are same
			List<IndexBlockV3Header> newDstBlocks = toList(logfile2.getIndexBlocks());

			for (int j = 0; j < f1Blocks.size(); ++j) {
				String digest = logfile1.getRawDataBlock(f1Blocks.get(j)).getDigest();
				String digest2 = logfile2.getRawDataBlock(newDstBlocks.get(j)).getDigest();

				assertEquals(digest, digest2);
			}

		} finally {
			StorageUtil.ensureClose(logfile2);
			StorageUtil.ensureClose(logfile1);
		}
	}

	@Test
	public void truncateTest() throws IOException {
		LogFileV3o logfile1 = null;
		LogFileV3o logfile2 = null;
		try {
			logfile1 = new LogFileV3o(truncIndexFile, truncDataFile);
			logfile2 = new LogFileV3o(truncTargetIndexFile, truncTargetDataFile);

			logfile1.truncate(2);
			assertEquals(2, logfile1.getIndexBlockCount());

			// check file length
			assertEquals(truncTargetIndexFile.length(), truncIndexFile.length());
			assertEquals(truncTargetDataFile.length(), truncDataFile.length());

			List<IndexBlockV3Header> f1Blocks = toList(logfile1.getIndexBlocks());
			List<IndexBlockV3Header> f2Blocks = toList(logfile2.getIndexBlocks());

			int firstDiff = getFirstDiffBlock(f1Blocks, f2Blocks);
			assertEquals(firstDiff, 2);

			for (int j = 0; j < f1Blocks.size(); ++j) {
				String digest = logfile1.getRawDataBlock(f1Blocks.get(j)).getDigest();
				String digest2 = logfile2.getRawDataBlock(f2Blocks.get(j)).getDigest();

				assertEquals(digest, digest2);
			}

			logfile1.truncate(5);
			assertEquals(2, logfile1.getIndexBlockCount());

			logfile1.truncate(0);
			assertEquals(0, logfile1.getIndexBlockCount());
		} finally {
			StorageUtil.ensureClose(logfile2);
			StorageUtil.ensureClose(logfile1);
		}
	}

	@Test
	public void truncateEmptyFileTest() throws IOException {
		LogFileV3o logfile = null;
		try {
			emptyIndexFile.delete();
			emptyDataFile.delete();

			logfile = new LogFileV3o(emptyIndexFile, emptyDataFile);

			logfile.truncate(3);

			assertEquals(0, logfile.getIndexBlockCount());

		} finally {
			StorageUtil.ensureClose(logfile);
		}

	}

	private int getFirstDiffBlock(List<IndexBlockV3Header> srcBlocks, List<IndexBlockV3Header> dstBlocks) {
		int lastIdenticalBlock = -1;
		for (int i = 0; i < dstBlocks.size(); ++i) {
			IndexBlockV3Header srcBlock = srcBlocks.get(i);
			IndexBlockV3Header dstBlock = dstBlocks.get(i);

			if (srcBlock.equals(dstBlock)) {
				lastIdenticalBlock = dstBlock.getId();
			} else {
				break;
			}
		}
		return lastIdenticalBlock + 1;
	}

	public static void copyLogFileV3(FilePath indexFile, FilePath dataFile, FilePath newIndexFile, FilePath newDataFile,
			Queue<Integer> incIds)
			throws IOException {
		LogFileV3o logfile = null;
		StorageOutputStream ios = null;
		StorageOutputStream dos = null;
		try {
			logfile = new LogFileV3o(indexFile, dataFile);

			long indexCount = logfile.getIndexBlockCount();

			ios = newIndexFile.newOutputStream(false);
			dos = newDataFile.newOutputStream(false);

			// XXX: copy headers
			logfile.writeIndexFileHeader(ios);
			assertEquals(logfile.getIndexFileHeaderLength(), ios.getPos());
			logfile.writeDataFileHeader(dos);
			assertEquals(logfile.getDataFileHeaderLength(), dos.getPos());

			logger.info("copy logfilev3: index block count: " + indexCount);
			List<IndexBlockV3Header> indexBlocks = toList(logfile.getIndexBlocks());

			int i = 0;
			for (IndexBlockV3Header indexBlock : indexBlocks) {
				if (incIds != null && incIds.isEmpty())
					break;

				assertEquals(logfile.getIndexFileHeaderLength() + i * IndexBlockV3Header.ITEM_SIZE, ios.getPos());
				assertEquals(indexBlock.getPosOnData(), dos.getPos());

				if (incIds != null && incIds.peek().equals((int) indexBlock.getId())) {
					incIds.poll();
					LogFileV3o.RawDataBlock rawBlock = logfile.getRawDataBlock(indexBlock);
					logger.info(
							"copy logfilev3: blockId: {}, raw digest: {}, dataFp: {}, dataLen: {}", new Object[] {
									indexBlock.getId(), rawBlock.getDigest(), indexBlock.getPosOnData(), indexBlock.getDataBlockLen() });

					// copy index block
					indexBlock.serialize(ios);

					// copy data block
					rawBlock.serialize(dos);
				} else {
					// reserve block
					indexBlock.newReservedBlock().serialize(ios);

					byte[] b = new byte[(int) indexBlock.getDataBlockLen().intValue()];
					dos.write(b);
				}
				i++;
				System.out.println((newIndexFile.length() - logfile.getIndexFileHeaderLength()) / IndexBlockV3Header.ITEM_SIZE);
				System.out.println((newIndexFile.length() - logfile.getIndexFileHeaderLength()) % IndexBlockV3Header.ITEM_SIZE);
			}

		} finally {
			StorageUtil.ensureClose(logfile);
			StorageUtil.ensureClose(ios);
			StorageUtil.ensureClose(dos);
		}

		assertTrue(indexFile.length() >= newIndexFile.length());
		assertTrue(dataFile.length() >= newDataFile.length());
	}

	static void genLogFile(FilePath indexFile, FilePath dataFile) throws InvalidLogFileHeaderException,
			IOException {

		LogWriterConfigV3o config = new LogWriterConfigV3o();
		config.setIndexPath(indexFile);
		config.setDataPath(dataFile);
		config.setTableName("lfsv3test");
		config.setListener(null);
		config.setFlushCount(200);
		config.setCompression("deflate");
		config.setCallbackSet(null);

		Random rand1 = new Random(1);
		Random rand2 = new Random(2);

		int count = 900;

		ArrayList<String> s1 = new ArrayList<String>(count);
		ArrayList<String> s2 = new ArrayList<String>(count);

		for (int i = 0; i < count; ++i) {
			s1.add("token" + rand1.nextInt(10000));
			s2.add("token" + rand2.nextInt(10000));
		}

		LogFileWriterV3o writer = null;
		try {
			writer = new LogFileWriterV3o(config);

			Stopwatch w = new Stopwatch();
			w.start();
			for (int i = 0; i < count; ++i) {
				Map<String, Object> logdata = new HashMap<String, Object>();
				logdata.put("line", String.format("%s,%s", s1.get(i), s2.get(i)));
				writer.write(new Log("lfwv3test", new Date(), i + 1, logdata));
			}
			System.out.println("input elapsed: " + w.elapsed(TimeUnit.MILLISECONDS));
		} finally {
			if (writer != null)
				writer.close();
		}

		System.out.println("idx len: " + indexFile.length() + " dat len: " + dataFile.length());
	}
}
