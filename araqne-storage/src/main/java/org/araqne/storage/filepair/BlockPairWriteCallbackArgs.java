package org.araqne.storage.filepair;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Date;

public class BlockPairWriteCallbackArgs {
	private String service;
	private String tableName;
	private Date day;
	private int id;
	private long indexBlockPos;
	private long dataBlockPos;
	private byte[] indexBlock;
	private byte[] dataBlock;

	public BlockPairWriteCallbackArgs(String service, String tableName, Date day,
			int blockId, long indexBlockPos, byte[] indexBlock, long dataBlockPos, byte[] dataBlock) {
		this.service = service;
		this.tableName = tableName;
		this.day = day;
		this.id = blockId;
		this.indexBlockPos = indexBlockPos;
		this.indexBlock = indexBlock;
		this.dataBlockPos = dataBlockPos;
		this.dataBlock = dataBlock;
	}

	@Override
	public String toString() {
		return String.format("BlockPairWriteCallbackArgs [svc=%s, tname=%s, day=%s, iblkpos=%d, iblk=%x, dblkpos=%d, dblk=%x]",
				service,
				tableName,
				day,
				indexBlockPos,
				new BigInteger(1, Arrays.copyOf(indexBlock, 10)),
				dataBlockPos,
				new BigInteger(1, Arrays.copyOf(dataBlock, 10)));
	}

	public String getService() {
		return service;
	}

	public String getTableName() {
		return tableName;
	}

	public Date getDay() {
		return day;
	}
	
	public int getId() {
		return id;
	}

	public long getIndexBlockPos() {
		return indexBlockPos;
	}

	public long getDataBlockPos() {
		return dataBlockPos;
	}

	public byte[] getIndexBlock() {
		return indexBlock;
	}

	public byte[] getDataBlock() {
		return dataBlock;
	}
}
