/*
 * Copyright 2010 NCHOVY
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
package org.araqne.log.api.msgbus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Requires;
import org.araqne.log.api.LogParserFactory;
import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.log.api.Logger;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.LoggerFactory;
import org.araqne.log.api.LoggerFactoryRegistry;
import org.araqne.log.api.LoggerRegistry;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.LoggerStartReason;
import org.araqne.log.api.LoggerStopReason;
import org.araqne.msgbus.Request;
import org.araqne.msgbus.Response;
import org.araqne.msgbus.handler.MsgbusMethod;
import org.araqne.msgbus.handler.MsgbusPlugin;

@Component(name = "logapi-logger-plugin")
@MsgbusPlugin
public class LoggerPlugin {
	@Requires
	private LoggerRegistry loggerRegistry;

	@Requires
	private LoggerFactoryRegistry loggerFactoryRegistry;

	@Requires
	private LogParserFactoryRegistry parserFactoryRegistry;

	@MsgbusMethod
	public void getLoggerFactories(Request req, Response resp) {
		Locale locale = req.getSession().getLocale();
		String s = req.getString("locale");
		if (s != null)
			locale = new Locale(s);

		resp.put("factories", Marshaler.marshal(loggerFactoryRegistry.getLoggerFactories(), locale));
	}

	@MsgbusMethod
	public void getParserFactories(Request req, Response resp) {
		Locale locale = req.getSession().getLocale();
		String s = req.getString("locale");
		if (s != null)
			locale = new Locale(s);

		List<Object> l = new ArrayList<Object>();

		for (String name : parserFactoryRegistry.getNames()) {
			LogParserFactory f = parserFactoryRegistry.get(name);
			l.add(Marshaler.marshal(f, locale));
		}

		resp.put("factories", l);
	}

	@SuppressWarnings("unchecked")
	@MsgbusMethod
	public void getLoggers(Request req, Response resp) {
		Set<String> loggerNames = null;

		if (req.get("logger_names") != null)
			loggerNames = new HashSet<String>((List<String>) req.get("logger_names"));

		List<Object> ret = new ArrayList<Object>();

		for (Logger logger : loggerRegistry.getLoggers()) {
			if (loggerNames != null && !loggerNames.contains(logger.getFullName()))
				continue;

			ret.add(Marshaler.marshal(logger));
		}

		resp.put("loggers", ret);
	}

	@MsgbusMethod
	public void getLogger(Request req, Response resp) {
		// logger fullname
		String loggerName = req.getString("logger_name");
		boolean includeConfigs = getBool(req, "include_configs");
		boolean includeStates = getBool(req, "include_states");

		Logger logger = loggerRegistry.getLogger(loggerName);
		if (logger != null)
			resp.put("logger", Marshaler.marshal(logger, includeConfigs, includeStates));
		else
			resp.put("logger", null);
	}

	private boolean getBool(Request req, String key) {
		if (req.getBoolean(key) == null)
			return false;
		return req.getBoolean(key);
	}

	@MsgbusMethod
	@SuppressWarnings("unchecked")
	public void resetLoggers(Request req, Response resp) {
		List<String> loggerNames = (List<String>) req.get("logger_names", true);
		for (String loggerName : loggerNames) {
			Logger logger = loggerRegistry.getLogger(loggerName);
			if (logger != null)
				logger.resetStates();
		}
	}

	@MsgbusMethod
	public void getFactoryOptions(Request req, Response resp) {
		String loggerFactoryName = req.getString("factory");
		Locale locale = req.getSession().getLocale();
		String s = req.getString("locale");
		if (s != null)
			locale = new Locale(s);

		LoggerFactory loggerFactory = loggerFactoryRegistry.getLoggerFactory(loggerFactoryName);
		resp.put("options", Marshaler.marshal(loggerFactory.getConfigOptions(), locale));
	}

	/**
	 * ensure updated logger configuration, and running state
	 */
	@MsgbusMethod
	public void ensureLoggerOperation(Request req, Response resp) {
		removeLogger(req, resp);
		createLogger(req, resp);
		startLogger(req, resp);
	}

	@MsgbusMethod
	public void createLogger(Request req, Response resp) {
		String loggerFactoryName = req.getString("factory");
		String loggerNamespace = req.getString("namespace");
		String loggerName = req.getString("name");
		String description = req.getString("description");

		if (req.has("logger")) {
			String loggerFullname = req.getString("logger");
			int pos = loggerFullname.indexOf('\\');
			if (pos < 0) {
				loggerNamespace = "local";
				loggerName = loggerFullname;
			} else {
				loggerNamespace = loggerFullname.substring(0, pos);
				loggerName = loggerFullname.substring(pos + 1);
			}
		}

		LoggerFactory loggerFactory = loggerFactoryRegistry.getLoggerFactory(loggerFactoryName);
		Map<String, String> config = new HashMap<String, String>();

		@SuppressWarnings("unchecked")
		Map<String, String> options = (Map<String, String>) req.get("options");
		for (LoggerConfigOption opt : loggerFactory.getConfigOptions()) {
			String value = null;

			try {
				value = options.get(opt.getName());
				if (value != null)
					config.put(opt.getName(), value);
			} catch (NullPointerException e) {
				if (opt.isRequired())
					throw e;
			}
		}

		LoggerSpecification spec = new LoggerSpecification();
		spec.setNamespace(loggerNamespace);
		spec.setName(loggerName);
		spec.setDescription(description);
		spec.setConfig(config);
		loggerFactory.newLogger(spec);
	}

	@MsgbusMethod
	public void removeLoggers(Request req, Response resp) {
		@SuppressWarnings("unchecked")
		List<String> loggers = (List<String>) req.get("loggers");

		for (String fullName : loggers) {
			Logger logger = loggerRegistry.getLogger(fullName);
			if (logger == null)
				continue;

			logger.stop(LoggerStopReason.USER_REQUEST);
			String[] tokens = fullName.split("\\\\");
			LoggerFactory factory = loggerFactoryRegistry.getLoggerFactory(logger.getFactoryNamespace(), logger.getFactoryName());
			factory.deleteLogger(tokens[0], tokens[1]);
		}
	}

	@MsgbusMethod
	public void removeLogger(Request req, Response resp) {
		String fullName = req.getString("logger");

		Logger logger = loggerRegistry.getLogger(fullName);
		if (logger == null)
			return;

		logger.stop(LoggerStopReason.USER_REQUEST);
		String[] tokens = fullName.split("\\\\");
		LoggerFactory factory = loggerFactoryRegistry.getLoggerFactory(logger.getFactoryNamespace(), logger.getFactoryName());
		factory.deleteLogger(tokens[0], tokens[1]);
	}

	@MsgbusMethod
	public void startLogger(Request req, Response resp) {
		String fullName = req.getString("logger");
		int interval = req.getInteger("interval");
		Logger logger = loggerRegistry.getLogger(fullName);
		if (logger == null)
			return;

		logger.start(LoggerStartReason.USER_REQUEST, interval);
	}

	@MsgbusMethod
	public void stopLogger(Request req, Response resp) {
		String fullName = req.getString("logger");
		int waitTime = 5000;
		if (req.has("wait_time"))
			waitTime = req.getInteger("wait_time");

		Logger logger = loggerRegistry.getLogger(fullName);
		if (logger == null)
			return;

		logger.stop(LoggerStopReason.USER_REQUEST, waitTime);
	}

}
