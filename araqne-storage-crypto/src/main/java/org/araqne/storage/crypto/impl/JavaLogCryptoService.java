package org.araqne.storage.crypto.impl;

import java.security.PrivateKey;
import java.security.PublicKey;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.storage.crypto.BlockCipher;
import org.araqne.storage.crypto.LogCryptoException;
import org.araqne.storage.crypto.LogCryptoService;
import org.araqne.storage.crypto.MacBuilder;
import org.araqne.storage.crypto.PkiCipher;

@Component(name="java-logcrypto-service")
@Provides
public class JavaLogCryptoService implements LogCryptoService {

	@Override
	public PkiCipher newPkiCipher(PublicKey publicKey) throws LogCryptoException {
		return new JavaPkiCipher(publicKey);
	}

	@Override
	public PkiCipher newPkiCipher(PublicKey publicKey, PrivateKey privateKey) throws LogCryptoException {
		return new JavaPkiCipher(publicKey, privateKey);
	}

	@Override
	public BlockCipher newBlockCipher(String algorithm, byte[] cipherKey) throws LogCryptoException {
		return new JavaBlockCipher(algorithm, cipherKey);
	}

	@Override
	public MacBuilder newMacBuilder(String algorithm, byte[] digestKey) throws LogCryptoException {
		return new JavaMacBuilder(algorithm, digestKey);
	}

}
