package org.araqne.storage.crypto;

public interface PkiCipher {
	byte[] encrypt(byte[] input) throws LogCryptoException;

	byte[] encrypt(byte[] input, int offset, int limit) throws LogCryptoException;

	byte[] decrypt(byte[] input) throws LogCryptoException;

	byte[] decrypt(byte[] input, int offset, int limit) throws LogCryptoException;
}
