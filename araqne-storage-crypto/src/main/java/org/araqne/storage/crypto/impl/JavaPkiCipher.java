package org.araqne.storage.crypto.impl;

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.araqne.storage.crypto.LogCryptoException;
import org.araqne.storage.crypto.PkiCipher;

public class JavaPkiCipher implements PkiCipher {

	private PublicKey publicKey;
	private PrivateKey privateKey;
	private ThreadLocal<Cipher> rsa;

	public JavaPkiCipher(PublicKey publicKey) throws LogCryptoException {
		this(publicKey, null);
	}

	public JavaPkiCipher(PublicKey publicKey, PrivateKey privateKey) throws LogCryptoException {
		this.publicKey = publicKey;
		this.privateKey = privateKey;

		try {
			// test if algorithm is available 
			Cipher.getInstance(JavaPkiCipher.this.publicKey.getAlgorithm());
		} catch (NoSuchAlgorithmException e) {
			throw new LogCryptoException(getKeyInfo(publicKey), e);
		} catch (NoSuchPaddingException e) {
			throw new LogCryptoException(getKeyInfo(publicKey), e);
		}
		
		rsa = new ThreadLocal<Cipher>() {
			@Override
			protected Cipher initialValue() {
				try {
					return Cipher.getInstance(JavaPkiCipher.this.publicKey.getAlgorithm()); 
				} catch (NoSuchAlgorithmException e) {
					throw new IllegalStateException(e);
				} catch (NoSuchPaddingException e) {
					throw new IllegalStateException(e);
				}
			}
		};
	}

	@Override
	public byte[] encrypt(byte[] input) throws LogCryptoException {
		if (publicKey == null)
			throw new IllegalArgumentException("public key is not supplied");

		try {
			rsa.get().init(Cipher.ENCRYPT_MODE, publicKey);
			return rsa.get().doFinal(input);
		} catch (InvalidKeyException e) {
			throw new LogCryptoException(getKeyInfo(publicKey), e);
		} catch (IllegalBlockSizeException e) {
			throw new LogCryptoException(getKeyInfo(publicKey), e);
		} catch (BadPaddingException e) {
			throw new LogCryptoException(getKeyInfo(publicKey), e);
		}
	}

	@Override
	public byte[] encrypt(byte[] input, int offset, int limit) throws LogCryptoException {
		if (publicKey == null)
			throw new IllegalArgumentException("public key is not supplied");

		try {
			rsa.get().init(Cipher.ENCRYPT_MODE, publicKey);
			return rsa.get().doFinal(input, offset, limit);
		} catch (InvalidKeyException e) {
			throw new LogCryptoException(getKeyInfo(publicKey), e);
		} catch (IllegalBlockSizeException e) {
			throw new LogCryptoException(getKeyInfo(publicKey), e);
		} catch (BadPaddingException e) {
			throw new LogCryptoException(getKeyInfo(publicKey), e);
		}
	}

	@Override
	public byte[] decrypt(byte[] input) throws LogCryptoException {
		if (privateKey == null)
			throw new IllegalArgumentException("private key is not supplied");

		try {
			rsa.get().init(Cipher.DECRYPT_MODE, privateKey);
			return rsa.get().doFinal(input);
		} catch (InvalidKeyException e) {
			throw new LogCryptoException(getKeyInfo(privateKey), e);
		} catch (IllegalBlockSizeException e) {
			throw new LogCryptoException(getKeyInfo(privateKey), e);
		} catch (BadPaddingException e) {
			throw new LogCryptoException(getKeyInfo(privateKey), e);
		}
	}

	private String getKeyInfo(Key key) {
		String algorithm = key.getAlgorithm();
		String format = key.getFormat();

		return algorithm + ", " + format;
	}

	@Override
	public byte[] decrypt(byte[] input, int offset, int limit) throws LogCryptoException {
		if (privateKey == null)
			throw new IllegalArgumentException("private key is not supplied");

		try {
			rsa.get().init(Cipher.DECRYPT_MODE, privateKey);
			return rsa.get().doFinal(input, offset, limit);
		} catch (InvalidKeyException e) {
			throw new LogCryptoException(getKeyInfo(privateKey), e);
		} catch (IllegalBlockSizeException e) {
			throw new LogCryptoException(getKeyInfo(privateKey), e);
		} catch (BadPaddingException e) {
			throw new LogCryptoException(getKeyInfo(privateKey), e);
		}
	}

}
