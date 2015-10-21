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
package org.araqne.log.api.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.api.Script;
import org.araqne.api.ScriptArgument;
import org.araqne.api.ScriptContext;
import org.araqne.api.ScriptOptionParser;
import org.araqne.api.ScriptOptionParser.ScriptOption;
import org.araqne.api.ScriptUsage;
import org.araqne.log.api.AbstractLogPipe;
import org.araqne.log.api.Log;
import org.araqne.log.api.LogParserFactory;
import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.log.api.LogParserProfile;
import org.araqne.log.api.LogParserRegistry;
import org.araqne.log.api.LogTransformerFactory;
import org.araqne.log.api.LogTransformerFactoryRegistry;
import org.araqne.log.api.LogTransformerProfile;
import org.araqne.log.api.LogTransformerRegistry;
import org.araqne.log.api.Logger;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.LoggerFactory;
import org.araqne.log.api.LoggerFactoryRegistry;
import org.araqne.log.api.LoggerRegistry;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.LoggerStartReason;
import org.araqne.log.api.LoggerStopReason;
import org.araqne.log.api.Mutable;
import org.araqne.log.api.PredicatesConfigType;
import org.araqne.log.api.TimeRange;
import org.araqne.log.api.WildcardMatcher;
import org.json.JSONConverter;
import org.json.JSONException;

import com.bethecoder.ascii_table.ASCIITable;
import com.bethecoder.ascii_table.impl.CollectionASCIITableAware;
import com.bethecoder.ascii_table.impl.PropertyColumn;

public class LogApiScript implements Script {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(LogApiScript.class.getName());
	private ScriptContext context;
	private LoggerFactoryRegistry loggerFactoryRegistry;
	private LoggerRegistry loggerRegistry;
	private LogParserFactoryRegistry parserFactoryRegistry;
	private LogTransformerFactoryRegistry transformerFactoryRegistry;
	private LogParserRegistry parserRegistry;
	private LogTransformerRegistry transformerRegistry;

	public LogApiScript(LoggerFactoryRegistry loggerFactoryRegistry, LoggerRegistry loggerRegistry,
			LogParserFactoryRegistry parserFactoryRegistry, LogTransformerFactoryRegistry transformerFactoryRegistry,
			LogParserRegistry parserRegistry, LogTransformerRegistry transformerRegistry) {
		this.loggerFactoryRegistry = loggerFactoryRegistry;
		this.loggerRegistry = loggerRegistry;
		this.parserFactoryRegistry = parserFactoryRegistry;
		this.transformerFactoryRegistry = transformerFactoryRegistry;
		this.parserRegistry = parserRegistry;
		this.transformerRegistry = transformerRegistry;
	}

	@Override
	public void setScriptContext(ScriptContext context) {
		this.context = context;
	}

	/**
	 * @since 2.6.0
	 */
	@ScriptUsage(description = "print last log of loggers", arguments = { @ScriptArgument(name = "name filter", type = "string", description = "name filter", optional = true) })
	public void lastLogs(String[] args) {
		String filter = null;
		if (args.length > 0)
			filter = args[0];

		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
		for (Logger logger : loggerRegistry.getLoggers()) {
			if (logger.getLastLog() == null)
				continue;

			if (filter != null && !logger.getFullName().contains(filter))
				continue;

			context.println("----");
			context.println("Logger [" + logger.getFullName() + "] Last Timestamp [" + df.format(logger.getLastLogDate()) + "]");
			context.println(logger.getLastLog().getParams());
		}
	}

	public void parsers(String[] args) {
		context.println("Log Parser Profiles");
		context.println("---------------------");
		for (LogParserProfile profile : parserRegistry.getProfiles()) {
			context.println(profile);
		}
	}

	@ScriptUsage(description = "create parser profile", arguments = {
			@ScriptArgument(name = "profile name", type = "string", description = "parser profile name"),
			@ScriptArgument(name = "factory name", type = "string", description = "parser factory name", autocompletion = LogParserFactoryAutoCompleter.class) })
	public void createParser(String[] args) throws InterruptedException {
		String name = args[0];
		String factoryName = args[1];

		LogParserFactory parserFactory = parserFactoryRegistry.get(factoryName);
		if (parserFactory == null) {
			context.println("parser factory not found");
			return;
		}

		LogParserProfile profile = new LogParserProfile();
		profile.setName(name);
		profile.setFactoryName(factoryName);

		Map<String, String> configs = profile.getConfigs();
		for (LoggerConfigOption option : parserFactory.getConfigOptions()) {
			setOption(configs, option);
			profile.setConfigs(configs);
		}

		parserRegistry.createProfile(profile);
		context.println("created");
	}

	@ScriptUsage(description = "remove parser profile", arguments = { @ScriptArgument(name = "profile name", type = "string", description = "profile name") })
	public void removeParser(String[] args) {
		parserRegistry.removeProfile(args[0]);
		context.println("removed");
	}

	@ScriptUsage(description = "print transformer profiles", arguments = { @ScriptArgument(name = "name", type = "string", description = "filter by name", optional = true) })
	public void transformers(String[] args) {
		String filter = null;
		String filtered = "";
		if (args.length > 0) {
			filter = args[0];
			filtered = " (filtered)";
		}

		context.println("Log Transformer Profiles" + filtered);
		context.println("--------------------------");
		List<LogTransformerProfile> profiles = transformerRegistry.getProfiles();
		Collections.sort(profiles);
		for (LogTransformerProfile profile : profiles) {
			if (filter != null && !profile.getName().contains(filter))
				continue;

			context.println(profile);
		}
	}

	@ScriptUsage(description = "create transformer profile", arguments = {
			@ScriptArgument(name = "profile name", type = "string", description = "transformer profile name"),
			@ScriptArgument(name = "factory name", type = "string", description = "transformer factory name", autocompletion = LogTransformerFactoryRegistry.class) })
	public void createTransformer(String[] args) throws InterruptedException {
		String name = args[0];
		String factoryName = args[1];

		LogTransformerProfile profile = new LogTransformerProfile();
		profile.setName(name);
		profile.setFactoryName(factoryName);

		LogTransformerFactory factory = transformerFactoryRegistry.getFactory(factoryName);
		if (factory == null) {
			context.println("transformer factory not found");
			return;
		}

		Map<String, String> configs = profile.getConfigs();
		for (LoggerConfigOption option : factory.getConfigOptions()) {
			setOption(configs, option);
		}

		transformerRegistry.createProfile(profile);
		context.println("created");
	}

	@ScriptUsage(description = "remove transformer profile", arguments = { @ScriptArgument(name = "profile name", type = "string", description = "profile name") })
	public void removeTransformer(String[] args) {
		transformerRegistry.removeProfile(args[0]);
		context.println("removed");
	}

	public static class LoggerFactoryListItem implements Comparable<LoggerFactoryListItem> {
		private LoggerFactory loggerFactory;

		public LoggerFactoryListItem(LoggerFactory loggerFactory) {
			this.loggerFactory = loggerFactory;
		}

		public String getFullName() {
			return loggerFactory.getFullName();
		}

		public String getName() {
			return loggerFactory.getName();
		}

		public String getDisplayName() {
			return loggerFactory.getDisplayName(Locale.ENGLISH);
		}

		public String getDescription() {
			return loggerFactory.getDescription(Locale.ENGLISH);
		}

		@Override
		public int compareTo(LoggerFactoryListItem o) {
			return loggerFactory.getFullName().compareTo(o.getFullName());
		}
	}

	public void loggerFactories(String[] args) {
		ScriptOptionParser sop = new ScriptOptionParser(args);
		ScriptOption verbOpt = sop.getOption("v", "verbose", false);
		List<String> argl = sop.getArguments();

		String filtered = "";
		if (!argl.isEmpty())
			filtered = " (filtered)";

		context.println("Logger Factories" + filtered);
		context.println("---------------------");

		Collection<LoggerFactory> loggerFactories = loggerFactoryRegistry.getLoggerFactories();
		List<LoggerFactoryListItem> filteredList = new ArrayList<LoggerFactoryListItem>(loggerFactories.size());
		for (LoggerFactory loggerFactory : loggerFactories) {
			if (argl.isEmpty())
				filteredList.add(new LoggerFactoryListItem(loggerFactory));
			else if (containsTokens(loggerFactory.getFullName(), argl))
				filteredList.add(new LoggerFactoryListItem(loggerFactory));
		}

		Collections.sort(filteredList);

		if (verbOpt != null) {
			context.println(ASCIITable.getInstance().getTable(
					new CollectionASCIITableAware<LoggerFactoryListItem>(filteredList, Arrays.asList("fullName", "displayName",
							"description"), Arrays.asList("l!factory name", "l!display name", "l!description"))));
		} else {
			context.println(ASCIITable.getInstance().getTable(
					new CollectionASCIITableAware<LoggerFactoryListItem>(filteredList, Arrays.asList("fullName", "displayName"),
							Arrays.asList("l!factory name", "l!display name"))));
		}
	}

	public static class LoggerListItem implements Comparable<LoggerListItem> {

		private Logger logger;

		public LoggerListItem(Logger logger) {
			this.logger = logger;
		}

		@Override
		public int compareTo(LoggerListItem o) {
			return logger.getFullName().compareTo(o.logger.getFullName());
		}

		// "name", "factoryFullName", "Running", "interval", "logCount",
		// "lastStartDate", "lastRunDate", "enabled"
		public String getName() {
			return logger.getName();
		}

		public String getFullName() {
			return logger.getFullName();
		}

		public String getFactoryName() {
			return logger.getFactoryName();
		}

		public String getFactoryFullName() {
			return logger.getFactoryFullName();
		}

		public String getStatus() {
			return logger.isRunning() ? "running" : "stopped";
		}

		public int getInterval() {
			return logger.getInterval();
		}

		public long getLogCount() {
			return logger.getLogCount();
		}

		public long getDropCount() {
			return logger.getDropCount();
		}

		public Date getLastStartDate() {
			return logger.getLastStartDate();
		}

		public Date getLastRunDate() {
			return logger.getLastRunDate();

		}

		public Date getLastLogDate() {
			return logger.getLastLogDate();
		}

		public String getStopReason() {
			LoggerStopReason reason = logger.getStopReason();
			if (reason == null)
				return "";
			return reason.toString().toLowerCase().replace('_', ' ');
		}

		public String getTemporaryFailure() {
			Throwable t = logger.getTemporaryFailure();
			if (t == null)
				return "";
			return t.getMessage() != null ? t.getMessage() : t.getClass().getName();
		}

		public boolean isEnabled() {
			return logger.isEnabled();
		}

	}

	public void loggers(String[] args) {
		ScriptOptionParser sop = new ScriptOptionParser(args);
		ScriptOption verbOpt = sop.getOption("v", "verbose", false);
		ScriptOption fullVerbOpt = sop.getOption("V", "full-verbose", false);
		ScriptOption factFilter = sop.getOption("f", "factory", true);

		List<String> argl = sop.getArguments();
		String filtered = "";
		if (!argl.isEmpty())
			filtered = " (filtered)";

		context.println("Loggers" + filtered);
		context.println("----------------------");

		List<LoggerListItem> filteredList = new ArrayList<LoggerListItem>(loggerRegistry.getLoggers().size());
		for (Logger logger : loggerRegistry.getLoggers()) {
			if (argl.size() == 0 && factFilter == null)
				filteredList.add(new LoggerListItem(logger));
			else {
				boolean matches = true;
				if (argl.size() > 0 && !containsTokens(logger.getFullName(), argl))
					matches = false;
				if (factFilter != null && !containsTokens(logger.getFactoryFullName(), factFilter.values))
					matches = false;
				if (matches)
					filteredList.add(new LoggerListItem(logger));
			}
		}

		if (filteredList.isEmpty())
			return;

		Collections.sort(filteredList);

		if (fullVerbOpt != null) {
			for (LoggerListItem logger : filteredList) {
				context.println(logger.toString());
			}
		} else if (verbOpt != null)
			context.println(ASCIITable.getInstance().getTable(
					new CollectionASCIITableAware<LoggerListItem>(filteredList, new PropertyColumn("fullName", "l!name"),
							new PropertyColumn("factoryFullName", "l!factory"), new PropertyColumn("status", "l!status"),
							new PropertyColumn("interval", "intvl.(ms)"), new PropertyColumn("logCount", "log count"),
							new PropertyColumn("dropCount", "drop"), new PropertyColumn("lastStartDate", "l!last start"),
							new PropertyColumn("lastRunDate", "l!last run"), new PropertyColumn("lastLogDate", "l!last log"),
							new PropertyColumn("stopReason", "stop reason"), new PropertyColumn("temporaryFailure", "l!error"))));
		else
			context.println(ASCIITable.getInstance().getTable(
					new CollectionASCIITableAware<LoggerListItem>(filteredList, new PropertyColumn("fullName", "l!name"),
							new PropertyColumn("factoryName", "l!factory"), new PropertyColumn("enabled", "l!enabled"),
							new PropertyColumn("status", "l!status"), new PropertyColumn("interval", "intvl.(ms)"),
							new PropertyColumn("logCount", "log count"), new PropertyColumn("lastLogDate", "l!last log"),
							new PropertyColumn("stopReason", "stop reason"), new PropertyColumn("temporaryFailure", "l!error"))));

	}

	private boolean containsTokens(String fullName, List<String> args) {
		if (fullName == null)
			return false;

		Pattern p = WildcardMatcher.buildPattern(args.get(0));
		if (p == null)
			return fullName.contains(args.get(0));

		Matcher m = p.matcher(fullName);
		return m.find();
	}

	@ScriptUsage(description = "print logger configuration", arguments = { @ScriptArgument(name = "logger fullname", type = "string", description = "logger fullname", autocompletion = LoggerAutoCompleter.class) })
	public void logger(String[] args) {
		String fullName = args[0];
		context.println("Logger [" + fullName + "]");
		printLine(fullName.length() + 10);
		Logger logger = loggerRegistry.getLogger(fullName);
		if (logger == null) {
			context.println("logger not found");
			return;
		}

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String lastLogDate = logger.getLastLogDate() != null ? dateFormat.format(logger.getLastLogDate()) : "N/A";
		String lastRunDate = logger.getLastRunDate() != null ? dateFormat.format(logger.getLastRunDate()) : "N/A";

		context.println(" * Description: " + logger.getDescription());
		context.println(" * Logger Factory: " + logger.getFactoryFullName());
		context.println(" * Status: " + logger.getStatus());
		context.println(" * Interval: " + logger.getInterval() + "ms");
		context.println(" * Time Range: " + (logger.getTimeRange() != null ? logger.getTimeRange() : "N/A"));
		context.println(" * Last Log: " + lastLogDate);
		context.println(" * Last Run: " + lastRunDate);
		context.println(" * Log Count: " + logger.getLogCount());
		context.println(" * Drop Count: " + logger.getDropCount());

		context.println("");

		context.println("Configuration");
		context.println("---------------");
		Map<String, String> props = logger.getConfigs();
		if (props != null) {
			for (Object key : props.keySet())
				context.println(" * " + key + ": " + props.get(key));
		}
	}

	private void printLine(int len) {
		for (int i = 0; i < len; i++)
			context.print('-');
		context.println();
	}

	@ScriptUsage(description = "print log parser factories", arguments = { @ScriptArgument(name = "name filter", type = "string", description = "filter by factory name", optional = true) })
	public void parserFactories(String[] args) {
		String filter = null;
		String filtered = "";
		if (args.length > 0) {
			filter = args[0];
			filtered = " (filtered)";
		}

		context.println("Log Parser Factories" + filtered);
		context.println("----------------------");

		for (String name : parserFactoryRegistry.getNames()) {
			if (filter != null && !name.contains(filter))
				continue;
			context.println(name);
		}
	}

	@ScriptUsage(description = "print log transformer factories", arguments = { @ScriptArgument(name = "name filter", type = "string", description = "filter by factory name", optional = true) })
	public void transformerFactories(String[] args) {
		String filter = null;
		String filtered = "";
		if (args.length > 0) {
			filter = args[0];
			filtered = " (filtered)";
		}

		context.println("Log Transformer Factories" + filtered);
		context.println("---------------------------");

		for (LogTransformerFactory f : transformerFactoryRegistry.getFactories()) {
			if (filter != null && !f.getName().contains(filter))
				continue;
			context.println(f.getName() + ": " + f);
		}
	}

	@ScriptUsage(description = "trace logger output", arguments = { @ScriptArgument(name = "logger name", type = "string", description = "logger fullname", autocompletion = LoggerAutoCompleter.class) })
	public void trace(String[] args) {
		Logger logger = loggerRegistry.getLogger(args[0]);
		if (logger == null) {
			context.println("logger not found");
			return;
		}

		ConsoleLogPipe p = new ConsoleLogPipe();
		logger.addLogPipe(p);

		try {
			context.println("tracing logger: " + logger);
			while (true) {
				context.readLine();
			}
		} catch (InterruptedException e) {
			context.println("interrupted");
		} finally {
			logger.removeLogPipe(p);
		}
	}

	private class ConsoleLogPipe extends AbstractLogPipe {
		@Override
		public void onLog(Logger logger, Log log) {
			context.println(logger.getFullName() + ": " + log.toString());
		}

	}

	@ScriptUsage(description = "start the logger", arguments = {
			@ScriptArgument(name = "logger fullname", type = "string", description = "the logger fullname to start", autocompletion = LoggerAutoCompleter.class),
			@ScriptArgument(name = "interval", type = "int", description = "sleep time of active logger thread in milliseconds. 60000ms by default. passive logger will ignore interval", optional = true) })
	public void startLogger(String[] args) {
		try {
			String fullName = args[0];
			int interval = 0;
			if (args.length > 1)
				interval = Integer.parseInt(args[1]);

			Logger logger = loggerRegistry.getLogger(fullName);
			if (logger == null) {
				context.println("logger not found");
				return;
			}

			if (logger.isPassive())
				logger.start(LoggerStartReason.USER_REQUEST);
			else if (interval > 0)
				logger.start(LoggerStartReason.USER_REQUEST, interval);
			else
				throw new IllegalStateException("cannot start logger, interval is required");
			context.println("logger started");
		} catch (NumberFormatException e) {
			context.println("interval should be number in milliseconds");
		} catch (IllegalStateException e) {
			if (e.getMessage().contains("pending"))
				context.println("cannot start logger, " + e.getMessage());
			else
				context.println(e.getMessage());
		}
	}

	@ScriptUsage(description = "start loggers at once, passive logger will ignore interval", arguments = {
			@ScriptArgument(name = "logger names", type = "string", description = "logger name wildcard expression"),
			@ScriptArgument(name = "interval", type = "int", description = "run interval in milliseconds, 5000 by default", optional = true) })
	public void startLoggers(String[] args) {
		if (!args[0].contains("*")) {
			context.println("logger name expression should contains one or more wildcard");
			return;
		}

		boolean useOldInterval = args.length == 1;
		Pattern pattern = WildcardMatcher.buildPattern(args[0]);
		int newInterval = 5000;
		try {
			if (args.length > 1) {
				newInterval = Integer.parseInt(args[1]);
			}
		} catch (NumberFormatException e) {
			// ignore
		}

		Matcher matcher = pattern.matcher("");
		// start passive logger first
		for (Logger logger : loggerRegistry.getLoggers()) {
			matcher.reset(logger.getFullName());
			if (!matcher.find())
				continue;

			try {
				if (logger.isPassive()) {
					if (logger.isRunning()) {
						context.println("logger [" + logger.getFullName() + "] is already started");
					} else {
						logger.start(LoggerStartReason.USER_REQUEST);
						context.println("logger [" + logger.getFullName() + "] started");
					}
				}
			} catch (Throwable t) {
				context.println("cannot start logger [" + logger.getFullName() + "], " + t.getMessage());
				slog.error("araqne log api: canont start logger " + logger.getFullName(), t);
			}
		}

		// then, start active loggers
		for (Logger logger : loggerRegistry.getLoggers()) {
			matcher.reset(logger.getFullName());
			if (!matcher.find())
				continue;

			try {
				if (!logger.isPassive()) {
					if (logger.isRunning()) {
						context.println("logger [" + logger.getFullName() + "] is already started");
					} else {
						int interval = logger.getInterval();
						if (interval == 0 || !useOldInterval)
							interval = newInterval;

						logger.start(LoggerStartReason.USER_REQUEST, interval);
						context.println("logger [" + logger.getFullName() + "] started with interval " + interval + "ms");
					}
				}
			} catch (Throwable t) {
				context.println("cannot start logger [" + logger.getFullName() + "], " + t.getMessage());
				slog.error("araqne log api: canont start logger " + logger.getFullName(), t);
			}
		}
	}

	@ScriptUsage(description = "enable the logger", arguments = {
			@ScriptArgument(name = "logger fullname", type = "string", description = "the logger fullname to enable", autocompletion = LoggerAutoCompleter.class),
			@ScriptArgument(name = "interval", type = "int", description = "sleep time of active logger thread in milliseconds. passive logger will ignore interval", optional = true) })
	public void enableLogger(String[] args) {
		String fullName = args[0];

		Logger logger = loggerRegistry.getLogger(fullName);
		if (logger == null) {
			context.println("logger not found");
			return;
		}

		try {
			if (args.length > 1)
				logger.setInterval(Integer.parseInt(args[1]));
		} catch (NumberFormatException e) {
			context.println("interval should be number");
			return;
		}

		logger.setEnabled(true);
		context.println("logger enabled");
	}

	@ScriptUsage(description = "enable loggers at once, passive logger will ignore interval", arguments = {
			@ScriptArgument(name = "logger names", type = "string", description = "logger name wildcard expression"),
			@ScriptArgument(name = "interval", type = "int", description = "run interval in milliseconds, 5000 by default", optional = true) })
	public void enableLoggers(String[] args) {
		if (!args[0].contains("*")) {
			context.println("logger name expression should contains one or more wildcard");
			return;
		}

		boolean useOldInterval = args.length == 1;
		Pattern pattern = WildcardMatcher.buildPattern(args[0]);
		int newInterval = 5000;
		try {
			if (args.length > 1) {
				newInterval = Integer.parseInt(args[1]);
			}
		} catch (NumberFormatException e) {
			context.println("interval should be number");
			return;
		}

		Matcher matcher = pattern.matcher("");
		// start passive logger first
		for (Logger logger : loggerRegistry.getLoggers()) {
			matcher.reset(logger.getFullName());
			if (!matcher.find())
				continue;

			try {
				if (logger.isPassive()) {
					if (logger.isEnabled()) {
						context.println("logger [" + logger.getFullName() + "] is already enabled");
					} else {
						logger.setEnabled(true);
						context.println("logger [" + logger.getFullName() + "] enabled");
					}
				}
			} catch (Throwable t) {
				context.println("cannot enable logger [" + logger.getFullName() + "], " + t.getMessage());
				slog.error("araqne log api: canont enable logger " + logger.getFullName(), t);
			}
		}

		// then, start active loggers
		for (Logger logger : loggerRegistry.getLoggers()) {
			matcher.reset(logger.getFullName());
			if (!matcher.find())
				continue;

			try {
				if (!logger.isPassive()) {
					if (logger.isEnabled()) {
						context.println("logger [" + logger.getFullName() + "] is already enabled");
					} else {
						int interval = logger.getInterval();
						if (interval == 0 || !useOldInterval)
							interval = newInterval;

						logger.setInterval(interval);
						logger.setEnabled(true);
						context.println("logger [" + logger.getFullName() + "] enabled with interval " + interval + "ms");
					}
				}
			} catch (Throwable t) {
				context.println("cannot enable logger [" + logger.getFullName() + "], " + t.getMessage());
				slog.error("araqne log api: canont enable logger " + logger.getFullName(), t);
			}
		}
	}

	@ScriptUsage(description = "stop the logger", arguments = {
			@ScriptArgument(name = "logger names", type = "string", description = "the logger name to stop", autocompletion = LoggerAutoCompleter.class),
			@ScriptArgument(name = "max wait time", type = "int", description = "max wait time in milliseconds, 5000 by default", optional = true) })
	public void stopLogger(String[] args) {
		try {
			int maxWaitTime = 5000;
			String name = args[0];
			if (args.length > 1)
				maxWaitTime = Integer.parseInt(args[1]);

			Logger logger = loggerRegistry.getLogger(name);
			if (logger == null) {
				context.println("logger not found");
				return;
			}

			if (!logger.isPassive())
				context.println("waiting...");

			logger.stop(LoggerStopReason.USER_REQUEST, maxWaitTime);
			context.println("logger stopped");
		} catch (Exception e) {
			context.println(e.getMessage());
		}
	}

	@ScriptUsage(description = "stop loggers at once", arguments = {
			@ScriptArgument(name = "logger", type = "string", description = "a logger name or many logger names separated by space"),
			@ScriptArgument(name = "max wait time", type = "int", description = "stop wait time in milliseconds, 5000 by default", optional = true) })
	public void stopLoggers(String[] args) {
		if (!args[0].contains("*")) {
			context.println("logger name expression should contains one or more wildcard");
			return;
		}

		Pattern pattern = WildcardMatcher.buildPattern(args[0]);
		int maxWaitTime = 5000;
		try {
			if (args.length > 1) {
				maxWaitTime = Integer.parseInt(args[1]);
			}
		} catch (NumberFormatException e) {
			// ignore
		}

		Matcher matcher = pattern.matcher("");

		// stop active loggers first
		for (Logger logger : loggerRegistry.getLoggers()) {
			matcher.reset(logger.getFullName());
			if (!matcher.find())
				continue;

			try {
				if (!logger.isPassive()) {
					if (!logger.isRunning()) {
						context.println("logger [" + logger.getFullName() + "] is not running");
					} else {
						logger.stop(LoggerStopReason.USER_REQUEST, maxWaitTime);
						context.println("logger [" + logger.getFullName() + "] stopped");
					}
				}
			} catch (Throwable t) {
				context.println("cannot start logger [" + logger.getFullName() + "], " + t.getMessage());
				slog.error("araqne log api: canont start logger " + logger.getFullName(), t);
			}
		}

		// then stop passive loggers
		for (Logger logger : loggerRegistry.getLoggers()) {
			matcher.reset(logger.getFullName());
			if (!matcher.find())
				continue;

			try {
				if (logger.isPassive()) {
					if (!logger.isRunning()) {
						context.println("logger [" + logger.getFullName() + "] is not running");
					} else {
						logger.stop(LoggerStopReason.USER_REQUEST);
						context.println("logger [" + logger.getFullName() + "] stopped");
					}
				}
			} catch (Throwable t) {
				context.println("cannot start logger [" + logger.getFullName() + "], " + t.getMessage());
				slog.error("araqne log api: canont start logger " + logger.getFullName(), t);
			}
		}

	}

	@ScriptUsage(description = "disable the logger", arguments = {
			@ScriptArgument(name = "logger names", type = "string", description = "the logger name to disable", autocompletion = LoggerAutoCompleter.class),
			@ScriptArgument(name = "max wait time", type = "int", description = "max wait time in milliseconds, 5000 by default", optional = true) })
	public void disableLogger(String[] args) {
		try {
			int maxWaitTime = 5000;
			String name = args[0];

			try {
				if (args.length > 1)
					maxWaitTime = Integer.parseInt(args[1]);
			} catch (NumberFormatException e) {
				context.println("interval should be number");
				return;
			}

			Logger logger = loggerRegistry.getLogger(name);
			if (logger == null) {
				context.println("logger not found");
				return;
			}

			if (!logger.isPassive())
				context.println("waiting...");

			logger.setMaxWaitTime(maxWaitTime);
			logger.setEnabled(false);
			context.println("logger disabled");
		} catch (Exception e) {
			context.println(e.getMessage());
		}
	}

	@ScriptUsage(description = "stop loggers at once", arguments = {
			@ScriptArgument(name = "logger", type = "string", description = "a logger name or many logger names separated by space"),
			@ScriptArgument(name = "max wait time", type = "int", description = "stop wait time in milliseconds, 5000 by default", optional = true) })
	public void disableLoggers(String[] args) {
		if (!args[0].contains("*")) {
			context.println("logger name expression should contains one or more wildcard");
			return;
		}

		Pattern pattern = WildcardMatcher.buildPattern(args[0]);
		int maxWaitTime = 5000;
		try {
			if (args.length > 1) {
				maxWaitTime = Integer.parseInt(args[1]);
			}
		} catch (NumberFormatException e) {
			context.println("interval should be number");
			return;
		}

		Matcher matcher = pattern.matcher("");

		// stop active loggers first
		for (Logger logger : loggerRegistry.getLoggers()) {
			matcher.reset(logger.getFullName());
			if (!matcher.find())
				continue;

			try {
				if (!logger.isPassive()) {
					if (!logger.isEnabled()) {
						context.println("logger [" + logger.getFullName() + "] is not enabled");
					} else {
						logger.setMaxWaitTime(maxWaitTime);
						logger.setEnabled(false);
						context.println("logger [" + logger.getFullName() + "] disabled");
					}
				}
			} catch (Throwable t) {
				context.println("cannot disable logger [" + logger.getFullName() + "], " + t.getMessage());
				slog.error("araqne log api: canont disable logger " + logger.getFullName(), t);
			}
		}

		// then stop passive loggers
		for (Logger logger : loggerRegistry.getLoggers()) {
			matcher.reset(logger.getFullName());
			if (!matcher.find())
				continue;

			try {
				if (logger.isPassive()) {
					if (!logger.isEnabled()) {
						context.println("logger [" + logger.getFullName() + "] is not enabled");
					} else {
						logger.setEnabled(false);
						context.println("logger [" + logger.getFullName() + "] disabled");
					}
				}
			} catch (Throwable t) {
				context.println("cannot disable logger [" + logger.getFullName() + "], " + t.getMessage());
				slog.error("araqne log api: canont disable logger " + logger.getFullName(), t);
			}
		}

	}

	@ScriptUsage(description = "create new logger", arguments = {
			@ScriptArgument(name = "logger factory name", type = "string", description = "logger factory name. try logapi.loggerFactories command.", autocompletion = LoggerFactoryAutoCompleter.class),
			@ScriptArgument(name = "logger namespace", type = "string", description = "new logger namespace"),
			@ScriptArgument(name = "logger name", type = "string", description = "new logger name"),
			@ScriptArgument(name = "description", type = "string", description = "the description of new logger", optional = true) })
	public void createLogger(String[] args) {
		try {
			String loggerFactoryName = args[0];
			String loggerNamespace = args[1];
			String loggerName = args[2];
			String description = (args.length > 3) ? args[3] : null;

			LoggerFactory loggerFactory = loggerFactoryRegistry.getLoggerFactory(loggerFactoryName);
			if (loggerFactory == null) {
				context.println("logger factory not found: " + loggerFactoryName);
				return;
			}

			Map<String, String> config = new HashMap<String, String>();
			for (LoggerConfigOption type : loggerFactory.getConfigOptions()) {
				setOption(config, type);
			}

			// transform?
			context.print("transformer (optional, enter to skip)? ");
			String transformerName = context.readLine().trim();
			if (!transformerName.isEmpty())
				config.put("transformer", transformerName);

			LoggerSpecification spec = new LoggerSpecification();
			spec.setNamespace(loggerNamespace);
			spec.setName(loggerName);
			spec.setDescription(description);
			spec.setConfig(config);

			Logger logger = loggerFactory.newLogger(spec);
			if (logger == null) {
				context.println("failed to create logger");
				return;
			}

			context.println("logger created: " + logger.toString());
		} catch (InterruptedException e) {
			context.println("interrupted");
		} catch (Exception e) {
			context.println(e.getMessage());
			slog.error("araqne log api: cannot create logger", e);
		}
	}

	@ScriptUsage(description = "update logger config", arguments = { @ScriptArgument(name = "logger fullname", type = "string", description = "the logger fullname", autocompletion = LoggerAutoCompleter.class) })
	public void updateLogger(String[] args) {
		try {
			String loggerFullName = args[0];
			Logger logger = loggerRegistry.getLogger(loggerFullName);
			if (logger == null) {
				context.println("logger not found");
				return;
			}

			if (logger.isRunning()) {
				context.println("logger is running");
				return;
			}

			LoggerFactory loggerFactory = loggerFactoryRegistry.getLoggerFactory(logger.getFactoryName());
			Map<String, String> configs = logger.getConfigs();
			for (LoggerConfigOption type : loggerFactory.getConfigOptions()) {
				if (type instanceof Mutable) {
					setOption(configs, type, configs.get(type.getName()));
				}
			}
			logger.updateConfigs(configs);
			context.println("logger updated: " + logger.toString());
		} catch (InterruptedException e) {
			context.println("interrupted");
		} catch (Exception e) {
			context.println(e.getMessage());
			slog.error("araqne log api: cannot update logger config", e);
		}
	}

	@ScriptUsage(description = "remove logger", arguments = { @ScriptArgument(name = "logger fullname", type = "string", description = "the logger fullname", autocompletion = LoggerAutoCompleter.class) })
	public void removeLogger(String[] args) {
		try {
			String fullName = args[0];
			Logger logger = loggerRegistry.getLogger(fullName);

			if (logger == null) {
				context.println("logger not found");
				return;
			}

			// stop logger
			logger.stop(LoggerStopReason.USER_REQUEST);

			String[] tokens = fullName.split("\\\\");

			LoggerFactory factory = loggerFactoryRegistry.getLoggerFactory(logger.getFactoryNamespace(), logger.getFactoryName());
			// factory can already removed from registry
			if (factory != null)
				factory.deleteLogger(tokens[0], tokens[1]);

			context.println("logger removed");
		} catch (Exception e) {
			context.println("error: " + e.getMessage());
			slog.error("araqne log api: cannot remove logger", e);
		}
	}

	@ScriptUsage(description = "set time range. logger only works for specified time range.", arguments = {
			@ScriptArgument(name = "logger fullname", type = "string", description = "the logger fullname", autocompletion = LoggerAutoCompleter.class),
			@ScriptArgument(name = "start time", type = "string", description = "HH:mm format", optional = true),
			@ScriptArgument(name = "end time", type = "string", description = "HH:mm format", optional = true) })
	public void setTimeRange(String[] args) {
		try {
			String fullName = args[0];
			Logger logger = loggerRegistry.getLogger(fullName);

			if (logger == null) {
				context.println("logger not found");
				return;
			}

			if (args.length >= 3) {
				logger.setTimeRange(new TimeRange(args[1], args[2]));
				context.println("set");
			} else {
				logger.setTimeRange(null);
				context.println("unset");
			}
		} catch (Exception e) {
			context.println("error: " + e.getMessage());
			slog.error("araqne log api: cannot set time range", e);
		}
	}

	private void setOption(Map<String, String> config, LoggerConfigOption type) throws InterruptedException {
		setOption(config, type, null);
	}

	private void setOption(Map<String, String> config, LoggerConfigOption type, String initialValue) throws InterruptedException {
		String directive = type.isRequired() ? "(required)" : "(optional)";

		if (type instanceof PredicatesConfigType) {
			context.println(type.getDisplayName(Locale.ENGLISH) + " " + directive + "");

			List<Object> predicates = new ArrayList<Object>();
			while (true) {
				context.print(" * Condition (enter to end)? ");
				String cond = context.readLine();
				if (cond.trim().isEmpty())
					break;

				context.print(" * Value (enter to end)? ");
				String value = context.readLine();
				if (value.trim().isEmpty())
					break;

				predicates.add(Arrays.asList(cond, value));
			}

			if (predicates.isEmpty() && type.isRequired())
				setOption(config, type, initialValue);

			try {
				config.put(type.getName(), JSONConverter.jsonize(predicates));
			} catch (JSONException e) {
				throw new IllegalStateException("jsonize failure", e);
			}

		} else {
			context.print(type.getDisplayName(Locale.ENGLISH) + " " + directive + "? ");
			String value = context.readLine(initialValue);
			if (!value.isEmpty())
				config.put(type.getName(), value);

			if (value.isEmpty()) {
				if (type.isRequired())
					setOption(config, type, initialValue);
				else
					config.put(type.getName(), null);
			}
		}
	}

	@ScriptUsage(description = "reset logger state", arguments = { @ScriptArgument(name = "logger name", type = "string", description = "namespace\\name format", autocompletion = LoggerAutoCompleter.class) })
	public void resetState(String[] args) {
		Logger logger = loggerRegistry.getLogger(args[0]);
		if (logger == null) {
			context.println("logger not found");
			return;
		}

		logger.resetStates();
		context.println("reset completed");
	}

	@ScriptUsage(description = "print logger dependencies", arguments = { @org.araqne.api.ScriptArgument(name = "logger name", type = "string", description = "namespace\\name format", autocompletion = LoggerAutoCompleter.class) })
	public void dependencies(String[] args) {
		String fullName = args[0];

		Set<String> dependencies = loggerRegistry.getDependencies(fullName);
		if (dependencies == null) {
			context.println("no dependencies");
			return;
		}

		Set<String> set = Collections.emptySet();
		Logger logger = loggerRegistry.getLogger(fullName);
		if (logger != null) {
			set = logger.getUnresolvedLoggers();
		}

		context.println("Loger Dependencies");
		context.println("--------------------");

		for (String loggerName : dependencies) {
			String status = set.contains(loggerName) ? "unresolved" : "resolved";
			context.println(loggerName + " -> " + status);
		}
	}
}
