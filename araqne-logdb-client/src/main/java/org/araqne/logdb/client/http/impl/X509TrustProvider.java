package org.araqne.logdb.client.http.impl;

import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.Security;
import java.security.cert.X509Certificate;

import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactorySpi;
import javax.net.ssl.X509TrustManager;

@SuppressWarnings("serial")
public class X509TrustProvider extends java.security.Provider {
	private static final String PROVIDER_NAME = "X509TrustProvider";

	private X509TrustProvider() {
		super(PROVIDER_NAME, 1.0, "Use when you want to bypass the certificate validation.");
		put("TrustManagerFactory.XTrust509", TrustManagerFactoryImpl.class.getName());
	}

	public static void trustAllCertificates() {
		if (Security.getProvider(PROVIDER_NAME) == null) {
			Security.insertProviderAt(new X509TrustProvider(), 1);
			Security.setProperty("ssl.TrustManagerFactory.algorithm", "XTrust509");
		}
	}

	public static class TrustManagerFactoryImpl extends TrustManagerFactorySpi {
		@Override
		protected TrustManager[] engineGetTrustManagers() {
			TrustManager[] tm = new TrustManager[] { new X509TrustManager() {
				@Override
				public void checkClientTrusted(final X509Certificate[] chain, final String authType) {
				}

				@Override
				public void checkServerTrusted(final X509Certificate[] chain, final String authType) {
				}

				@Override
				public X509Certificate[] getAcceptedIssuers() {
					return null;
				}
			} };

			return tm;
		}

		@Override
		protected void engineInit(KeyStore ks) throws KeyStoreException {
		}

		@Override
		protected void engineInit(ManagerFactoryParameters spec) throws InvalidAlgorithmParameterException {
		}

	}
}
