package org.araqne.storage.crypto;

import java.security.PrivateKey;
import java.security.PublicKey;

public interface LogCryptoService {
	PkiCipher newPkiCipher(PublicKey publicKey) throws LogCryptoException;

	PkiCipher newPkiCipher(PublicKey publicKey, PrivateKey privateKey) throws LogCryptoException;

	BlockCipher newBlockCipher(String algorithm, byte[] cipherKey) throws LogCryptoException;

	MacBuilder newMacBuilder(String algorithm, byte[] digestKey) throws LogCryptoException;
}
