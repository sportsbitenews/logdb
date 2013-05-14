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
package org.araqne.logdb.client.http;

import org.araqne.logdb.client.LogDbSession;
import org.araqne.logdb.client.LogDbTransport;
import org.araqne.logdb.client.http.impl.CometSession;

/**
 * @since 0.5.0
 * @author xeraph
 * 
 */
public class CometTransport implements LogDbTransport {

	@Override
	public LogDbSession newSession(String host, int port) {
		return new CometSession(host, port);
	}

}