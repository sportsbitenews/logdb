package org.araqne.logstorage.exporter;

public class LogBlock {
	private int blockSize;
	private byte version;
	private byte optionFlag;
	private long maxTime;
	private long minTime;
	private long maxId;
	private long minId;
	private int originalBlockSize;
	private int compressedBlockSize;
	private int logOffsetLength;
	private byte[] logData;
	private int[] logOffsets;
	private byte[] iv;
	private byte[] signature;

	public byte[] getIv() {
		return iv;
	}

	public void setIv(byte[] iv) {
		this.iv = iv;
	}

	public byte[] getSignature() {
		return signature;
	}

	public void setSignature(byte[] signature) {
		this.signature = signature;
	}

	public int[] getLogOffsets() {
		return logOffsets;
	}

	public void setLogOffsets(int[] logOffsets) {
		this.logOffsets = logOffsets;
	}

	public byte[] getLogData() {
		return logData;
	}

	public void setLogData(byte[] logData) {
		this.logData = logData;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

	public byte getVersion() {
		return version;
	}

	public void setVersion(byte version) {
		this.version = version;
	}

	public byte getOptionFlag() {
		return optionFlag;
	}

	public void setOptionFlag(byte optionFlag) {
		this.optionFlag = optionFlag;
	}

	public long getMaxTime() {
		return maxTime;
	}

	public void setMaxTime(long maxTime) {
		this.maxTime = maxTime;
	}

	public long getMinTime() {
		return minTime;
	}

	public void setMinTime(long minTime) {
		this.minTime = minTime;
	}

	public long getMaxId() {
		return maxId;
	}

	public void setMaxId(long maxId) {
		this.maxId = maxId;
	}

	public long getMinId() {
		return minId;
	}

	public void setMinId(long minId) {
		this.minId = minId;
	}

	public int getOriginalBlockSize() {
		return originalBlockSize;
	}

	public void setOriginalBlockSize(int originalBlockSize) {
		this.originalBlockSize = originalBlockSize;
	}

	public int getCompressedBlockSize() {
		return compressedBlockSize;
	}

	public void setCompressedBlockSize(int compressedBlockSize) {
		this.compressedBlockSize = compressedBlockSize;
	}

	public int getLogOffsetLength() {
		return logOffsetLength;
	}

	public void setLogOffsetLength(int logOffsetLength) {
		this.logOffsetLength = logOffsetLength;
	}
}
