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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 아라크네 메시지버스 RPC 전문을 표현합니다.
 * 
 * @author xeraph@eediom.com
 * 
 */
public class Message {
	/**
	 * 아라크네 메시지버스 RPC 전문의 유형을 표현합니다.
	 * 
	 * @author xeraph
	 * 
	 */
	public enum Type {
		/**
		 * 사용되지 않음
		 */
		None,

		/**
		 * RPC 요청
		 */
		Request,

		/**
		 * RPC 응답
		 */
		Response,

		/**
		 * 비동기 트랩 (서버 푸시)
		 */
		Trap;
	}

	private String guid;
	private Type type;
	private String session;
	private String requestId;
	private String source;
	private String target;
	private String method;
	private Map<String, Object> parameters;

	private String errorCode;
	private String errorMessage;

	public Message() {
		parameters = new HashMap<String, Object>();
		guid = UUID.randomUUID().toString();
		type = Type.Request;
		parameters = new HashMap<String, Object>();
		source = "0";
		target = "0";
	}

	public String getGuid() {
		return guid;
	}

	public void setGuid(String guid) {
		this.guid = guid;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public String getSession() {
		return session;
	}

	public void setSession(String session) {
		this.session = session;
	}

	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getTarget() {
		return target;
	}

	public void setTarget(String target) {
		this.target = target;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public Map<String, Object> getParameters() {
		return parameters;
	}

	public void setParameters(Map<String, Object> parameters) {
		this.parameters = parameters;
	}

	public Object get(String key) {
		return parameters.get(key);
	}

	public boolean containsKey(String key) {
		return parameters.containsKey(key);
	}

	public Long getLong(String key) {
		if (!parameters.containsKey(key))
			return null;

		Object v = parameters.get(key);
		if (v == null)
			return null;

		if (v instanceof Integer)
			return (long) ((Integer) v);
		else
			return (Long) v;
	}

	public Integer getInt(String key) {
		if (!parameters.containsKey(key))
			return null;

		return (Integer) parameters.get(key);
	}

	public String getString(String key) {
		if (!parameters.containsKey(key))
			return null;

		return (String) parameters.get(key);
	}

	public Boolean getBoolean(String key) {
		if (!parameters.containsKey(key))
			return null;

		return (Boolean) parameters.get(key);
	}

	public Date getDate(String key) {
		if (!parameters.containsKey(key))
			return null;

		return (Date) parameters.get(key);
	}

	public String getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(String errorCode) {
		this.errorCode = errorCode;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	@Override
	public String toString() {
		return String.format("header [guid=%s, session=%d, source=%s, target=%s, method=%s, error=%s]", guid, session, source,
				target, method, errorMessage);
	}

}
