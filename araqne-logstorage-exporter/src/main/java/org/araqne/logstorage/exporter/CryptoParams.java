package org.araqne.logstorage.exporter;

public class CryptoParams {
	private String cipher;
	private String digest;
	private byte[] cipherKey;
	private byte[] digestKey;

	public String getCipher() {
		return cipher;
	}

	public void setCipher(String cipher) {
		this.cipher = cipher;
	}

	public String getDigest() {
		return digest;
	}

	public void setDigest(String digest) {
		this.digest = digest;
	}

	public byte[] getCipherKey() {
		return cipherKey;
	}

	public void setCipherKey(byte[] cipherKey) {
		this.cipherKey = cipherKey;
	}

	public byte[] getDigestKey() {
		return digestKey;
	}

	public void setDigestKey(byte[] digestKey) {
		this.digestKey = digestKey;
	}
}
