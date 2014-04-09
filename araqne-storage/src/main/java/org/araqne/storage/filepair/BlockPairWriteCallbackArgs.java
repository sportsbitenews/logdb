package org.araqne.storage.filepair;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Date;

public class BlockPairWriteCallbackArgs {
	private String service;
	private String tableName;
	private Date day;
	private byte[] indexBlock;
	private byte[] dataBlock;

	public BlockPairWriteCallbackArgs(String service, String tableName, Date day, byte[] indexBlock, byte[] dataBlock) {
		this.service = service;
		this.tableName = tableName;
		this.day = day;
		this.indexBlock = indexBlock;
		this.dataBlock = dataBlock;
	}

	@Override
	public String toString() {
		return String.format("BlockPairWriteCallbackArgs [svc=%s, tname=%s, day=%s, iblk=%x, dblk=%x]", 
				service,
				tableName, 
				day, 
				new BigInteger(1, Arrays.copyOf(indexBlock, 10)), 
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

	public byte[] getIndexBlock() {
		return indexBlock;
	}

	public byte[] getDataBlock() {
		return dataBlock;
	}

}
