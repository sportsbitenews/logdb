package org.araqne.storage.crypto.impl;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.araqne.storage.crypto.BlockCipher;
import org.araqne.storage.crypto.LogCryptoException;

public class JavaBlockCipher implements BlockCipher {

	private String algorithm;
	private byte[] cipherKey;
	private ThreadLocal<Cipher> c;
	private SecretKeySpec cipherKeySpec;

	public JavaBlockCipher(String algorithm, byte[] cipherKey) throws LogCryptoException {
		this.algorithm = algorithm;
		this.cipherKey = cipherKey;
		
		try {
			// test if algorithm is available 
			Cipher.getInstance(algorithm);
		} catch (NoSuchAlgorithmException e) {
			throw new LogCryptoException(algorithm, e);
		} catch (NoSuchPaddingException e) {
			throw new LogCryptoException(algorithm, e);
		}
		
		c = new ThreadLocal<Cipher>() {
			@Override
			protected Cipher initialValue() {
				try {
					return Cipher.getInstance(JavaBlockCipher.this.algorithm); 
				} catch (NoSuchAlgorithmException e) {
					throw new IllegalStateException(e);
				} catch (NoSuchPaddingException e) {
					throw new IllegalStateException(e);
				}
			}
		};
		
		cipherKeySpec = new SecretKeySpec(cipherKey, algorithm.split("[/-]")[0]);
	}

	@Override
	public byte[] encrypt(byte[] iv, byte[] input) throws LogCryptoException {
		if (cipherKey == null)
			throw new IllegalArgumentException("cipher key is not supplied");

		try {
			IvParameterSpec ivSpec = new IvParameterSpec(iv);
			c.get().init(Cipher.ENCRYPT_MODE, cipherKeySpec, ivSpec);
			return c.get().doFinal(input);
		} catch (InvalidKeyException e) {
			throw new LogCryptoException(algorithm, e);
		} catch (IllegalBlockSizeException e) {
			throw new LogCryptoException(algorithm, e);
		} catch (BadPaddingException e) {
			throw new LogCryptoException(algorithm, e);
		} catch (InvalidAlgorithmParameterException e) {
			throw new LogCryptoException(algorithm, e);
		}

	}

	@Override
	public byte[] encrypt(byte[] iv, byte[] input, int offset, int limit)
			throws LogCryptoException {
		if (cipherKey == null)
			throw new IllegalArgumentException("cipher key is not supplied");

		try {
			IvParameterSpec ivSpec = new IvParameterSpec(iv);
			c.get().init(Cipher.ENCRYPT_MODE, cipherKeySpec, ivSpec);
			return c.get().doFinal(input, offset, limit);
		} catch (InvalidKeyException e) {
			throw new LogCryptoException(algorithm, e);
		} catch (IllegalBlockSizeException e) {
			throw new LogCryptoException(algorithm, e);
		} catch (BadPaddingException e) {
			throw new LogCryptoException(algorithm, e);
		} catch (InvalidAlgorithmParameterException e) {
			throw new LogCryptoException(algorithm, e);
		}
	}

	@Override
	public byte[] decrypt(byte[] iv, byte[] input) throws LogCryptoException {
		if (cipherKey == null)
			throw new IllegalArgumentException("cipher key is not supplied");

		try {
			IvParameterSpec ivSpec = new IvParameterSpec(iv);
			c.get().init(Cipher.DECRYPT_MODE, cipherKeySpec, ivSpec);
			return c.get().doFinal(input);
		} catch (InvalidKeyException e) {
			throw new LogCryptoException(algorithm, e);
		} catch (IllegalBlockSizeException e) {
			throw new LogCryptoException(algorithm, e);
		} catch (BadPaddingException e) {
			throw new LogCryptoException(algorithm, e);
		} catch (InvalidAlgorithmParameterException e) {
			throw new LogCryptoException(algorithm, e);
		}
	}

	@Override
	public byte[] decrypt(byte[] iv, byte[] input, int offset, int limit)
			throws LogCryptoException {
		if (cipherKey == null)
			throw new IllegalArgumentException("cipher key is not supplied");

		try {
			IvParameterSpec ivSpec = new IvParameterSpec(iv);
			c.get().init(Cipher.DECRYPT_MODE, cipherKeySpec, ivSpec);
			return c.get().doFinal(input, offset, limit);
		} catch (InvalidKeyException e) {
			throw new LogCryptoException(algorithm, e);
		} catch (IllegalBlockSizeException e) {
			throw new LogCryptoException(algorithm, e);
		} catch (BadPaddingException e) {
			throw new LogCryptoException(algorithm, e);
		} catch (InvalidAlgorithmParameterException e) {
			throw new LogCryptoException(algorithm, e);
		}
	}

}
