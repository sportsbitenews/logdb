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
import java.util.Map;

import org.araqne.logdb.client.http.impl.TrapListener;

/**
 * @since 0.5.0
 * @author xeraph
 * 
 */
public interface LogDbSession {
	boolean isClosed();

	void login(String loginName, String password) throws IOException;

	void login(String loginName, String password, boolean force) throws IOException;

	void logout() throws IOException;

	Message rpc(String method) throws IOException;

	Message rpc(String method, Map<String, Object> params) throws IOException;

	Message rpc(String method, int timeout) throws IOException;

	Message rpc(String method, Map<String, Object> params, int timeout) throws IOException;

	Message rpc(Message req, int timeout) throws IOException;

	void registerTrap(String callbackName) throws IOException;

	void unregisterTrap(String callbackName) throws IOException;

	void addListener(TrapListener listener);

	void removeListener(TrapListener listener);

	void close();

}
