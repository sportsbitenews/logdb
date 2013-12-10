package org.araqne.logstorage.exporter;

import java.util.Arrays;

public class LogFileHeader {

	private String magicString;
	private short bom;
	private short version;
	private short headerSize;
	private byte[] ext;

	public String getMagicString() {
		return magicString;
	}

	public void setMagicString(String magicString) {
		this.magicString = magicString;
	}

	public short getBom() {
		return bom;
	}

	public void setBom(short bom) {
		this.bom = bom;
	}

	public short getVersion() {
		return version;
	}

	public void setVersion(short version) {
		this.version = version;
	}

	public short getHeaderSize() {
		return headerSize;
	}

	public void setHeaderSize(short headerSize) {
		this.headerSize = headerSize;
	}

	public byte[] getExt() {
		return ext;
	}

	public void setExt(byte[] ext) {
		this.ext = ext;
	}

	@Override
	public String toString() {
		return "LogFileHeader [magicString=" + magicString + ", bom=" + bom + ", version=" + version + ", headerSize="
				+ headerSize + ", ext=" + Arrays.toString(ext) + "]";
	}
}
