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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.httpd.HttpContext;
import org.araqne.httpd.HttpService;
import org.araqne.log.api.http.HttpPostListener;
import org.araqne.log.api.http.HttpPostLog;
import org.araqne.log.api.http.HttpPostLogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "http-post-log-service")
@Provides
public class HttpPostLogServiceImpl extends HttpServlet implements HttpPostLogService {
	private static final long serialVersionUID = 1L;

	private final Logger slog = LoggerFactory.getLogger(HttpPostLogServiceImpl.class);

	@Requires
	private HttpService httpd;

	// callback name to listener mappings
	private ConcurrentHashMap<String, Set<HttpPostListener>> callbackMap;

	@Validate
	public void start() {
		HttpContext context = httpd.ensureContext("webconsole");
		context.addServlet("log", this, "/log/*");

		callbackMap = new ConcurrentHashMap<String, Set<HttpPostListener>>();
	}

	@Invalidate
	public void stop() {
		if (httpd != null) {
			HttpContext context = httpd.ensureContext("webconsole");
			context.removeServlet("log");
		}

		callbackMap.clear();
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		if (slog.isDebugEnabled()) {
			slog.debug("araqne logapi http: post path [{}] content-type [{}], content-length [{}]",
					new Object[] { req.getPathInfo(), req.getContentType(), req.getContentLength() });
		}

		ByteArrayOutputStream bos = new ByteArrayOutputStream(8096);
		byte[] b = new byte[8096];
		while (true) {
			int len = req.getInputStream().read(b);
			if (len < 0)
				break;

			bos.write(b, 0, len);
		}

		String callback = req.getPathInfo().substring(1);

		Set<HttpPostListener> set = callbackMap.get(callback);
		if (set == null)
			return;

		for (HttpPostListener listener : set) {
			try {
				HttpPostLog log = new HttpPostLog();
				log.setCallback(callback);
				log.setClientIp(req.getRemoteAddr());
				log.setData(bos.toString("utf-8"));
				listener.onPost(log);
			} catch (Throwable t) {
				slog.warn("araqne logapi http: post listener should not throw any exception", t);
			}
		}
	}

	@Override
	public void addListener(String callback, HttpPostListener listener) {
		Set<HttpPostListener> s = new CopyOnWriteArraySet<HttpPostListener>();
		Set<HttpPostListener> old = callbackMap.putIfAbsent(callback, s);
		if (old != null)
			s = old;

		s.add(listener);
	}

	@Override
	public void removeListener(String callback, HttpPostListener listener) {
		Set<HttpPostListener> s = new CopyOnWriteArraySet<HttpPostListener>();
		Set<HttpPostListener> old = callbackMap.putIfAbsent(callback, s);
		if (old != null)
			s = old;

		s.remove(listener);
	}

}
