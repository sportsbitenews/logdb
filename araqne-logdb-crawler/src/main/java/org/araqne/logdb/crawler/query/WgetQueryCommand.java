/**
 * Copyright 2014 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb.crawler.query;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.araqne.api.Io;
import org.araqne.codec.Base64;
import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.Strings;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xeraph@eediom.com
 */
public class WgetQueryCommand extends DriverQueryCommand {
	private static long WGET_MAX_SIZE;

	static {
		WGET_MAX_SIZE = 10485760;

		// override default max size
		String s = System.getProperty("araqne.logdb.wget_max_size");
		if (s != null) {
			try {
				WGET_MAX_SIZE = Long.valueOf(s);
			} catch (NumberFormatException e) {
			}
		}
	}

	private final Logger slog = LoggerFactory.getLogger(WgetQueryCommand.class);
	private final TrustManager[] trustAllCerts = new TrustManager[] { new IgnoreTrustManager() };
	private final HostnameVerifier hostnameVerifier = new IgnoreHostnameVerifier();

	private String url;
	private String selector;
	private int timeout;
	private String method;
	private String encoding;
	private String auth;
	private String authHeader;
	private boolean isHttps;

	public WgetQueryCommand(String url, String selector, int timeout, String method, String encoding, String auth) {
		this.url = url;
		this.selector = selector;
		this.timeout = timeout;
		this.method = method;
		this.encoding = encoding;
		this.auth = auth;
		if (auth != null) {
			try {
				String base64Auth = new String(Base64.encode(auth.getBytes("utf-8")));
				this.authHeader = "Basic " + base64Auth;
			} catch (UnsupportedEncodingException e) {
				// unreachable
			}
		}

		this.isHttps = false;
		try {
			new URL(url).getProtocol().equals("https");
		} catch (MalformedURLException e) {
		}
	}

	@Override
	public String getName() {
		return "wget";
	}

	@Override
	public boolean isDriver() {
		return url != null;
	}

	@Override
	public void run() {
		try {
			Row row = new Row();
			if (selector != null)
				fetchUrlByJsoup(row, url);
			else
				fetchUrl(row, url, true);

			pushPipe(row);
		} catch (Throwable t) {
			slog.debug("araqne logdb crawler: wget failed - " + url, t);
			throw new IllegalStateException("wget: " + t.getMessage());
		}
	}

	@Override
	public void onPush(Row row) {
		Object o = row.get("url");
		String url = null;
		try {
			if (o == null)
				return;

			url = o.toString();

			if (selector != null)
				fetchUrlByJsoup(row, url);
			else
				fetchUrl(row, url, false);
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne logdb crawler: wget failed - " + url, t);
		} finally {
			pushPipe(row);
		}
	}

	private void fetchUrl(Row row, String url, boolean throwException) throws Exception {
		HttpURLConnection conn = null;
		InputStream is = null;
		byte[] b = new byte[8096];
		ByteArrayOutputStream bos = new ByteArrayOutputStream(10240);
		int total = 0;
		try {
			conn = (HttpURLConnection) new URL(url).openConnection();
			if (conn instanceof HttpsURLConnection) {
				final SSLContext sslContext = SSLContext.getInstance("SSL");
				sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
				((HttpsURLConnection) conn).setSSLSocketFactory(sslContext.getSocketFactory());
				((HttpsURLConnection) conn).setHostnameVerifier(hostnameVerifier);
			}

			conn.setConnectTimeout(timeout * 1000);
			conn.setReadTimeout(timeout * 1000);
			conn.setRequestMethod(method.toUpperCase());
			if (authHeader != null)
				conn.setRequestProperty("Authorization", authHeader);

			is = conn.getInputStream();

			while (true) {
				int len = is.read(b);
				if (len < 0)
					break;

				total += len;
				if (total >= WGET_MAX_SIZE) {
					if (throwException)
						throw new IllegalStateException("Too large HTTP response, exceeds max size " + WGET_MAX_SIZE);
					
					row.put("_wget_error", "exceeds-max-size");
					return;
				}

				bos.write(b, 0, len);
			}

			row.put("line", bos.toString(encoding));
		} finally {
			Io.ensureClose(is);
			conn.disconnect();
		}
	}

	private void fetchUrlByJsoup(Row row, String url) throws Exception {
		SSLSocketFactory oldSocketFactory = null;
		HostnameVerifier oldHostnameVerifier = null;
		try {
			if (isHttps) {
				oldSocketFactory = HttpsURLConnection.getDefaultSSLSocketFactory();
				oldHostnameVerifier = HttpsURLConnection.getDefaultHostnameVerifier();

				final SSLContext sslContext = SSLContext.getInstance("SSL");
				sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
				HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
				HttpsURLConnection.setDefaultHostnameVerifier(hostnameVerifier);
			}

			Connection conn = Jsoup.connect(url);
			if (authHeader != null)
				conn.header("Authorization", authHeader);

			conn.ignoreContentType(true);
			conn.timeout(timeout * 1000);
			Document doc = null;

			if (method.equals("get"))
				doc = conn.get();
			else if (method.equals("post"))
				doc = conn.post();

			if (doc != null) {
				if (selector != null) {
					Elements elements = doc.select(selector);
					ArrayList<Object> l = new ArrayList<Object>(elements.size());

					for (Element e : elements) {
						Map<String, Object> m = new HashMap<String, Object>();

						for (Attribute attr : e.attributes()) {
							m.put(attr.getKey(), attr.getValue());
						}

						m.put("own_text", e.ownText());
						m.put("text", e.text());
						l.add(m);
					}

					row.put("elements", l);
				} else {
					row.put("html", doc.outerHtml());
				}
			}
		} finally {
			if (isHttps) {
				HttpsURLConnection.setDefaultSSLSocketFactory(oldSocketFactory);
				HttpsURLConnection.setDefaultHostnameVerifier(oldHostnameVerifier);
			}
		}
	}

	@Override
	public String toString() {
		String s = "wget";
		if (url != null)
			s += " url=\"" + url + "\"";

		if (selector != null)
			s += " selector=\"" + selector + "\"";

		if (timeout != 30000)
			s += " timeout=" + timeout;

		if (!method.equals("get"))
			s += " method=" + method;

		if (!encoding.equals("utf-8"))
			s += " encoding=" + encoding;

		if (auth != null)
			s += " auth=" + Strings.doubleQuote(auth);

		return s;
	}

	static class IgnoreTrustManager implements X509TrustManager {
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
	}

	static class IgnoreHostnameVerifier implements HostnameVerifier {
		@Override
		public boolean verify(String s, SSLSession sslSession) {
			return true;
		}
	}
}
