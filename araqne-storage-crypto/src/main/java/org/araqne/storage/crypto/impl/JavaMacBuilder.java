package org.araqne.storage.crypto.impl;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.araqne.storage.crypto.LogCryptoException;
import org.araqne.storage.crypto.MacBuilder;

public class JavaMacBuilder implements MacBuilder {

	private String algorithm;
	private byte[] digestKey;
	private ThreadLocal<Mac> c;
	private SecretKeySpec digestKeySpec;

	public JavaMacBuilder(String algorithm, byte[] digestKey) throws LogCryptoException {
		this.algorithm = algorithm;
		this.digestKey = digestKey;
		
		try {
			// test if algorithm is available 
			Mac.getInstance(algorithm);
		} catch (NoSuchAlgorithmException e) {
			throw new LogCryptoException(algorithm, e);
		}
		
		c = new ThreadLocal<Mac>() {
			@Override
			protected Mac initialValue() {
				try {
					return Mac.getInstance(JavaMacBuilder.this.algorithm); 
				} catch (NoSuchAlgorithmException e) {
					throw new IllegalStateException(e);
				}
			}
		};
		
		digestKeySpec = new SecretKeySpec(digestKey, algorithm);
	}

	@Override
	public byte[] digest(byte[] input) throws LogCryptoException {
		if (digestKey == null)
			throw new IllegalArgumentException("cipher key is not supplied");

		try {
			c.get().init(digestKeySpec);
			return c.get().doFinal(input);
		} catch (InvalidKeyException e) {
			throw new LogCryptoException(algorithm, e);
		}
	}

	@Override
	public byte[] digest(byte[] input, int offset, int limit) throws LogCryptoException {
		if (digestKey == null)
			throw new IllegalArgumentException("cipher key is not supplied");

		try {
			c.get().init(digestKeySpec);
			c.get().update(input, offset, limit);
			return c.get().doFinal();
		} catch (InvalidKeyException e) {
			throw new LogCryptoException(algorithm, e);
		}
	}

}
