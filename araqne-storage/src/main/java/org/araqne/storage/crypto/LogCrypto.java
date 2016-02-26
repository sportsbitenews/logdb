package org.araqne.storage.crypto;

import java.security.PrivateKey;
import java.security.PublicKey;

public interface LogCrypto {
	BlockCipher newEncryptor(String cipher);
	
	BlockCipher newDecryptor(String cipher);
	
	MacBuilder newMacBuilder(String algorithm);
	
	byte[] encrypt(byte[] b, PublicKey publicKey) throws LogCryptoException;

	byte[] decrypt(byte[] b, PrivateKey privateKey) throws LogCryptoException;

	byte[] encrypt(byte[] input, int limit, String cipher, byte[] cipherKey, byte[] iv) throws LogCryptoException;

	byte[] decrypt(byte[] input, String cipher, byte[] cipherKey, byte[] iv) throws LogCryptoException;

	byte[] digest(byte[] input, int limit, String digest, byte[] digestKey) throws LogCryptoException;
	
	
}
