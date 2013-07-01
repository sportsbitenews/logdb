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
package org.araqne.logdb.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;

import org.araqne.logdb.client.http.impl.TrapListener;

/**
 * @since 0.5.0
 * @author xeraph
 * 
 */
public abstract class AbstractLogDbSession implements LogDbSession {
	protected boolean isClosed;
	protected CopyOnWriteArraySet<TrapListener> listeners = new CopyOnWriteArraySet<TrapListener>();

	@Override
	public boolean isClosed() {
		return isClosed;
	}

	@Override
	public void login(String loginName, String password) throws IOException {
		login(loginName, password, false);
	}

	@Override
	public void login(String loginName, String password, boolean force) throws IOException {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("login_name", loginName);
		params.put("password", password);

		rpc("org.araqne.logdb.msgbus.ManagementPlugin.login", params);
	}

	@Override
	public void logout() throws IOException {
		rpc("org.araqne.logdb.msgbus.ManagementPlugin.logout");
	}

	@Override
	public Message rpc(String method) throws IOException {
		return rpc(method, 0);
	}

	@Override
	public Message rpc(String method, Map<String, Object> params) throws IOException {
		return rpc(method, params, 0);
	}

	@Override
	public Message rpc(String method, int timeout) throws IOException {
		Message req = new Message();
		req.setMethod(method);
		return rpc(req, timeout);
	}

	@Override
	public Message rpc(String method, Map<String, Object> params, int timeout) throws IOException {
		Message req = new Message();
		req.setMethod(method);
		req.setParameters(params);
		return rpc(req, timeout);
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

	public void close() throws IOException {
		try {
			if (isClosed())
				return;

			isClosed = true;

			// do not wait
			rpc("org.araqne.logdb.msgbus.ManagementPlugin.logout", 1);
		} catch (Throwable t) {
		}
	}
}
