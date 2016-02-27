package org.araqne.storage.crypto;

public interface BlockCipher {
	byte[] encrypt(byte[] iv, byte[] input) throws LogCryptoException;

	byte[] encrypt(byte[] iv, byte[] input, int offset, int limit)
			throws LogCryptoException;

	byte[] decrypt(byte[] iv, byte[] input) throws LogCryptoException;

	byte[] decrypt(byte[] iv, byte[] input, int offset, int limit)
			throws LogCryptoException;
}
