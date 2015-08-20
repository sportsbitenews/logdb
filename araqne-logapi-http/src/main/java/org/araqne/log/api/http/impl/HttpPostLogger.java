/*
 * Copyright 2015 Eediom Inc.
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
package org.araqne.log.api.http.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.AbstractLogger;
import org.araqne.log.api.LoggerFactory;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.LoggerStartReason;
import org.araqne.log.api.LoggerStopReason;
import org.araqne.log.api.Reconfigurable;
import org.araqne.log.api.SimpleLog;
import org.araqne.log.api.http.HttpPostListener;
import org.araqne.log.api.http.HttpPostLog;
import org.araqne.log.api.http.HttpPostLogService;

public class HttpPostLogger extends AbstractLogger implements HttpPostListener, Reconfigurable {

	private HttpPostLogService postLog;

	public HttpPostLogger(LoggerSpecification spec, LoggerFactory loggerFactory, HttpPostLogService postLog) {
		super(spec, loggerFactory);
		this.postLog = postLog;
	}

	@Override
	public boolean isPassive() {
		return true;
	}

	@Override
	protected void runOnce() {
	}

	@Override
	public void onConfigChange(Map<String, String> oldConfigs, Map<String, String> newConfigs) {
	}

	@Override
	protected void onStart(LoggerStartReason reason) {
		String callback = getConfigs().get("callback");
		postLog.addListener(callback, this);
	}

	@Override
	protected void onStop(LoggerStopReason reason) {
		String callback = getConfigs().get("callback");
		postLog.removeListener(callback, this);
	}

	@Override
	public void onPost(HttpPostLog log) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("callback", log.getCallback());
		m.put("client_ip", log.getClientIp());
		m.put("line", log.getData());

		write(new SimpleLog(new Date(), getFullName(), m));
	}

}
