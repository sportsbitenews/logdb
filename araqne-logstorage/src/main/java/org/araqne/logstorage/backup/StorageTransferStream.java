package org.araqne.logstorage.backup;

import java.io.InputStream;

public class StorageTransferStream {
	// original table name for stream write
	private String tableName;

	// original table id for stream write
	private int tableId;

	// use input stream and filename instead of storage input file
	private InputStream inputStream;

	private String mediaFileName;

	public StorageTransferStream(String tableName, int tableId, InputStream is, String mediaFileName) {
		this.tableName = tableName;
		this.tableId = tableId;
		this.inputStream = is;
		this.mediaFileName = mediaFileName;
	}

	public String getTableName() {
		return tableName;
	}

	public int getTableId() {
		return tableId;
	}

	public InputStream getInputStream() {
		return inputStream;
	}

	public String getMediaFileName() {
		return mediaFileName;
	}

}
