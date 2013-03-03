/*
 * Copyright 2013 Eediom Inc.
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
package org.araqne.logdb.client.http.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;

import org.araqne.logdb.client.Message;
import org.araqne.logdb.client.MessageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Session implements TrapListener {
	private final Logger logger = LoggerFactory.getLogger(Session.class);
	private String host;
	private String cookie;
	private TrapReceiver trapReceiver;
	private CopyOnWriteArraySet<TrapListener> listeners = new CopyOnWriteArraySet<TrapListener>();

	public Session(String host) {
		this.host = host;
	}

	public void login(String loginName, String password) throws IOException {
		login(loginName, password, false);
	}

	public void login(String loginName, String password, boolean force) throws IOException {
		Message hello = rpc("org.araqne.dom.msgbus.LoginPlugin.hello");
		String nonce = hello.getString("nonce");
		String h = hashPassword(password, nonce);

		Map<String, Object> params = new HashMap<String, Object>();
		params.put("nick", loginName);
		params.put("hash", h);
		params.put("force", force);

		rpc("org.araqne.dom.msgbus.LoginPlugin.login", params);

		trapReceiver = new TrapReceiver(host, cookie);
		trapReceiver.addListener(this);
		trapReceiver.start();
	}

	public void logout() throws IOException {
		rpc("org.araqne.dom.msgbus.LoginPlugin.logout");
	}

	private String hashPassword(String password, String nonce) {
		return Sha1.hash(Sha1.hash(password) + nonce);
	}

	public Message rpc(String method) throws IOException {
		Message req = new Message();
		req.setMethod(method);
		return rpc(req);
	}

	public Message rpc(String method, Map<String, Object> params) throws IOException {
		Message req = new Message();
		req.setMethod(method);
		req.setParameters(params);
		return rpc(req);
	}

	private Message rpc(Message req) throws IOException {
		String json = MessageCodec.encode(req);

		HttpURLConnection con = null;
		InputStream is = null;

		try {
			con = (HttpURLConnection) new URL("http://" + host + "/msgbus/request").openConnection();
			con.setRequestMethod("POST");
			con.setDoOutput(true);
			if (cookie != null) {
				con.setRequestProperty("Cookie", cookie);
			}

			con.getOutputStream().write(json.getBytes("utf-8"));

			is = con.getInputStream();
			if (con.getHeaderField("Set-Cookie") != null) {
				cookie = con.getHeaderField("Set-Cookie").split(";")[0];
			}

			ByteArrayOutputStream bos = null;
			if (con.getContentLength() > 0)
				bos = new ByteArrayOutputStream(con.getContentLength());
			else
				bos = new ByteArrayOutputStream();

			byte[] b = new byte[8096];

			while (true) {
				int read = is.read(b);
				if (read < 0)
					break;

				bos.write(b, 0, read);
			}

			String text = new String(bos.toByteArray(), "utf-8");
			Message msg = MessageCodec.decode(text);

			if (msg.getErrorCode() != null)
				throw new MessageException(msg.getErrorCode(), msg.getErrorMessage(), msg.getParameters());

			return msg;
		} finally {
			if (is != null)
				is.close();

			if (con != null)
				con.disconnect();
		}
	}

	public void registerTrap(String callbackName) throws IOException {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("callback", callbackName);
		rpc("org.araqne.msgbus.PushPlugin.subscribe", params);
	}

	public void unregisterTrap(String callbackName) throws IOException {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("callback", callbackName);
		rpc("org.araqne.msgbus.PushPlugin.unsubscribe", params);
	}

	public void addListener(TrapListener listener) {
		listeners.add(listener);
	}

	public void removeListener(TrapListener listener) {
		listeners.remove(listener);
	}

	@Override
	public void onTrap(Message msg) {
		for (TrapListener listener : listeners) {
			try {
				listener.onTrap(msg);
			} catch (Throwable t) {
				logger.error("logdb client: trap listener should not throw any exception", t);
			}
		}
	}

	@Override
	public void onClose(Throwable t) {
		trapReceiver = null;
		
		for (TrapListener listener : listeners) {
			try {
				listener.onClose(t);
			} catch (Throwable t2) {
				logger.error("logdb client: trap listener should not throw any exception", t2);
			}
		}
	}

	public void close() {
		if (trapReceiver != null) {
			trapReceiver.close();
			trapReceiver = null;
		}
	}
}
