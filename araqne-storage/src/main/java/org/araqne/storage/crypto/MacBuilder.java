package org.araqne.storage.crypto;

public interface MacBuilder {
	byte[] digest(byte[] input) throws LogCryptoException;

	byte[] digest(byte[] input, int offset, int limit) throws LogCryptoException;
}
