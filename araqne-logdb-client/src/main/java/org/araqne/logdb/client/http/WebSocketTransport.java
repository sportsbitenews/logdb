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

import java.io.IOException;

import org.araqne.logdb.client.LogDbSession;
import org.araqne.logdb.client.LogDbTransport;
import org.araqne.logdb.client.http.impl.WebSocketSession;

/**
 * 메시지버스 RPC 웹 소켓 전송 계층을 구현합니다.
 * 
 * @since 0.5.0
 * @author xeraph@eediom.com
 * 
 */
public class WebSocketTransport implements LogDbTransport {

	private boolean secure;

	/**
	 * 평문 통신을 수행하는 웹소켓 전송 계층을 생성합니다.
	 */
	public WebSocketTransport() {
		this(false);
	}

	/**
	 * 암호화 채널을 통하여 접속하려는 경우 생성자 매개변수를 true로 전달합니다.
	 * 
	 * @param secure
	 *            wss:// 스키마를 사용하는 경우 true
	 * @since 0.9.7
	 */
	public WebSocketTransport(boolean secure) {
		this.secure = secure;
	}

	@Override
	public LogDbSession newSession(String host, int port) throws IOException {
		return new WebSocketSession(host, port, secure);
	}
	
	@Override
	public LogDbSession newSession(String host, int port, int connectTimeout) throws IOException {
		return new WebSocketSession(host, port, secure, connectTimeout);
	}

	@Override
	public LogDbSession newSession(String host, int port, int connectTimeout, int readTimeout) throws IOException {
		return new WebSocketSession(host, port, secure, connectTimeout, readTimeout);
	}

}
