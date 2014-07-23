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

/**
 * @since 2.1.0
 * @author xeraph
 */
public class LogStats {
	private String tableName;
	private int logCount;
	private int blockSize;
	private int originalDataSize;
	private int compressedDataSize;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getLogCount() {
		return logCount;
	}

	public void setLogCount(int logCount) {
		this.logCount = logCount;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

	public int getOriginalDataSize() {
		return originalDataSize;
	}

	public void setOriginalDataSize(int originalDataSize) {
		this.originalDataSize = originalDataSize;
	}

	public int getCompressedDataSize() {
		return compressedDataSize;
	}

	public void setCompressedDataSize(int compressedDataSize) {
		this.compressedDataSize = compressedDataSize;
	}

}
