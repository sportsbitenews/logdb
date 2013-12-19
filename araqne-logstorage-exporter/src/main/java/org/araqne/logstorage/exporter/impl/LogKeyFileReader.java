package org.araqne.logstorage.exporter.impl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.PrivateKey;

import org.araqne.codec.Base64;
import org.araqne.logstorage.Crypto;
import org.araqne.logstorage.exporter.CryptoParams;
import org.araqne.logstorage.exporter.LogBlock;

public class LogKeyFileReader {

	public static CryptoParams getCryptoParams(File keyPath, File pfxFile, String password) {
		KeyStore keystore = getKeystore(pfxFile, password);
		try {
			if (keyPath.exists() && keyPath.length() > 0) {
				CryptoParams params = new CryptoParams();
				byte[] b = readAllBytes(keyPath);
				b = Crypto.decrypt(b, getPrivateKey(keystore, password));

				String line = new String(b);
				String[] tokens = line.split(",");
				if (!tokens[0].equals("v1"))
					throw new IllegalStateException("unsupported key file version: " + tokens[0]);

				params.setCipher(tokens[1].isEmpty() ? null : tokens[1]);
				params.setDigest(tokens[2].isEmpty() ? null : tokens[2]);
				params.setCipherKey(tokens[3].isEmpty() ? null : Base64.decode(tokens[3]));
				params.setDigestKey(tokens[4].isEmpty() ? null : Base64.decode(tokens[4]));
				return params;
			}
		} catch (Exception e) {
			throw new IllegalStateException("cannot load key file", e);
		}
		return null;
	}

	private static PrivateKey getPrivateKey(KeyStore keystore, String password) {
		try {
			String alias = keystore.aliases().nextElement();
			return (PrivateKey) keystore.getKey(alias, password.toCharArray());
		} catch (Exception e) {
			throw new IllegalStateException("cannot load public key of crypto profile ", e);
		}
	}

	private static byte[] readAllBytes(File keyPath) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		FileInputStream is = null;
		try {
			is = new FileInputStream(keyPath);
			byte[] b = new byte[8096];
			while (true) {
				int count = is.read(b);
				if (count < 0)
					break;
				bos.write(b, 0, count);
			}

			return bos.toByteArray();
		} finally {
			if (is != null)
				is.close();
		}
	}

	private static KeyStore getKeystore(File pfxFile, String password) {
		FileInputStream is = null;
		try {
			KeyStore pfx = KeyStore.getInstance("PKCS12");
			is = new FileInputStream(pfxFile);
			pfx.load(is, password.toCharArray());
			return pfx;
		} catch (Exception e) {
			throw new IllegalStateException("cannot load pfx file [" + pfxFile.getAbsolutePath() + "] of crypto profile ", e);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
				}
			}
		}
	}
}
