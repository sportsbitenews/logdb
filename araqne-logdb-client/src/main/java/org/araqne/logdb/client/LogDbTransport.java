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

/**
 * 로그 DB 트랜스포트는 주어진 접속 정보로 새로운 로그프레소 세션 개체를 생성합니다.
 * 
 * @since 0.5.0
 * @author xeraph@eediom.com
 * 
 */
public interface LogDbTransport {
	/**
	 * 새 세션을 생성합니다.
	 * 
	 * @param host
	 *            접속할 로그프레소 서버의 IP 주소 혹은 도메인 주소
	 * @param port
	 *            접속할 로그프레소 서버의 웹 포트 번호
	 * @param timeout 
	 * @return 새 세션 개체
	 */
	LogDbSession newSession(String host, int port) throws IOException;

	LogDbSession newSession(String host, int port, int timeout) throws IOException;
}
