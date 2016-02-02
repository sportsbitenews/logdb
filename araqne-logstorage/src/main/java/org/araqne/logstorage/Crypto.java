package org.araqne.logstorage;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Random;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class Crypto {
	public static byte[] encrypt(byte[] b, PublicKey publicKey) throws Exception {
		Cipher rsa = Cipher.getInstance("RSA");
		rsa.init(Cipher.ENCRYPT_MODE, publicKey);
		return rsa.doFinal(b);
	}

	public static byte[] decrypt(byte[] b, PrivateKey privateKey) throws Exception {
		Cipher rsa = Cipher.getInstance("RSA");
		rsa.init(Cipher.DECRYPT_MODE, privateKey);
		return rsa.doFinal(b);
	}

	public static byte[] encrypt(byte[] input, int limit, String cipher, byte[] cipherKey, byte[] iv) throws Exception {
		Cipher c = Cipher.getInstance(cipher);
		SecretKeySpec keySpec = new SecretKeySpec(cipherKey, cipher.split("[/-]")[0]);
		IvParameterSpec ivSpec = new IvParameterSpec(iv);
		c.init(Cipher.ENCRYPT_MODE, keySpec, ivSpec);
		return c.doFinal(input, 0, limit);
	}

	public static byte[] decrypt(byte[] input, String cipher, byte[] cipherKey, byte[] iv) throws Exception {
		Cipher decrypt = Cipher.getInstance(cipher);
		SecretKeySpec keySpec = new SecretKeySpec(cipherKey, cipher.split("[/-]")[0]);
		IvParameterSpec ivSpec = new IvParameterSpec(iv);

		decrypt.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);
		return decrypt.doFinal(input);
	}

	public static byte[] digest(byte[] input, int limit, String digest, byte[] digestKey) throws Exception {
		Mac hmac = Mac.getInstance(digest);
		hmac.init(new SecretKeySpec(digestKey, digest));
		hmac.update(input, 0, limit);
		return hmac.doFinal();
	}
}
