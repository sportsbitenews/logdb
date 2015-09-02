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
package org.araqne.log.api;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.araqne.api.DateFormat;

public abstract class AbstractLogger implements Logger, Runnable {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(AbstractLogger.class.getName());
	private static final int INFINITE = 0;
	private String fullName;
	private String namespace;
	private String name;
	private String factoryFullName;
	private String factoryNamespace;
	private String factoryName;
	private String description;
	private CopyOnWriteArraySet<LogPipe> pipes;
	private Thread t;
	private int interval;
	private TimeRange timeRange;

	private Map<String, String> config;

	private volatile LoggerStatus status = LoggerStatus.Stopped;
	private volatile boolean enabled = false;
	private volatile boolean doStop = false;
	private volatile boolean stopped = true;
	private volatile boolean pending = false;
	private volatile boolean manualStart = false;
	private volatile boolean stopCallbacked = false;

	private volatile Date lastStartDate;
	private volatile Date lastRunDate;
	private volatile Date lastLogDate;
	private volatile Date lastWriteDate;
	private volatile Log lastLog;
	private AtomicLong logCounter;
	private AtomicLong dropCounter;

	private Set<LoggerEventListener> eventListeners;
	private LogTransformer transformer;
	private LoggerFactory factory;
	private LoggerStopReason lastStopReason;
	private Set<String> unresolvedLoggers = new CopyOnWriteArraySet<String>();
	private Throwable temporaryFailure;

	/**
	 * @since 1.7.0
	 */
	public AbstractLogger(LoggerSpecification spec, LoggerFactory factory) {
		// logger factory info
		this.factoryNamespace = factory.getNamespace();
		this.factoryName = factory.getName();
		this.factoryFullName = factoryNamespace + "\\" + factoryName;
		this.factory = factory;

		// logger info
		this.namespace = spec.getNamespace();
		this.name = spec.getName();
		this.fullName = namespace + "\\" + name;
		this.description = spec.getDescription();
		this.config = spec.getConfig();

		reloadStates();

		this.pipes = new CopyOnWriteArraySet<LogPipe>();
		this.eventListeners = new CopyOnWriteArraySet<LoggerEventListener>();
	}

	@Override
	public String getFullName() {
		return fullName;
	}

	@Override
	public String getNamespace() {
		return namespace;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String getFactoryFullName() {
		return factoryFullName;
	}

	@Override
	public String getFactoryName() {
		return factoryName;
	}

	@Override
	public String getFactoryNamespace() {
		return factoryNamespace;
	}

	@Override
	public String getDescription() {
		return description;
	}

	@Override
	public boolean isPassive() {
		return false;
	}

	@Override
	public Date getLastStartDate() {
		return lastStartDate;
	}

	@Override
	public Date getLastRunDate() {
		return lastRunDate;
	}

	@Override
	public Date getLastLogDate() {
		return lastLogDate;
	}

	@Override
	public Date getLastWriteDate() {
		return lastWriteDate;
	}

	@Override
	public Log getLastLog() {
		return lastLog;
	}

	@Override
	public LoggerStopReason getStopReason() {
		return lastStopReason;
	}

	public Throwable getTemporaryFailure() {
		return temporaryFailure;
	}

	public void setTemporaryFailure(Throwable temporaryFailure) {
		boolean changed = false;

		Throwable lhs = this.temporaryFailure;
		Throwable rhs = temporaryFailure;

		if (lhs == null && rhs == null) {
			changed = false;
		} else if (checkOneSideNull(lhs, rhs)) {
			changed = true;
		} else if (checkOneSideNull(lhs.getMessage(), rhs.getMessage())
				|| ((lhs.getMessage() != null && rhs.getMessage() != null && !lhs.getMessage().equals(rhs.getMessage())))) {
			changed = true;
		}

		this.temporaryFailure = temporaryFailure;

		if (changed) {
			for (LoggerEventListener callback : eventListeners) {
				try {
					callback.onFailureChange(this);
				} catch (Throwable t) {
					slog.warn("araqne log api; logger [" + getFullName() + "] callback should not throw any exception", t);
				}
			}
		}
	}

	private boolean checkOneSideNull(Object lhs, Object rhs) {
		return (lhs != null && rhs == null) || (lhs == null && rhs != null);
	}

	@Override
	public long getLogCount() {
		return logCounter.get();
	}

	@Override
	public long getDropCount() {
		return dropCounter.get();
	}

	@Override
	public long getUpdateCount() {
		LastStateService lss = factory.getLastStateService();
		LastState state = lss.getState(getFullName());
		if (state == null)
			return 0;
		return state.getUpdateCount();
	}

	@Override
	public boolean isEnabled() {
		return enabled;
	}

	@Override
	public boolean isRunning() {
		return !stopped;
	}

	public boolean isPending() {
		return pending;
	}

	@Override
	public void setPending(boolean pending) {
		this.pending = pending;
		if (pending) {
			if (!unresolvedLoggers.isEmpty())
				lastStopReason = LoggerStopReason.LOGGER_DEPENDENCY;
			else if (config.get("transformer") != null && transformer == null)
				lastStopReason = LoggerStopReason.TRANSFORMER_DEPENDENCY;
		}
	}

	@Override
	public boolean isManualStart() {
		return manualStart;
	}

	@Override
	public void setManualStart(boolean manualStart) {
		this.manualStart = manualStart;
		notifyConfigChange();
	}

	@Override
	public LoggerStatus getStatus() {
		return status;
	}

	@Override
	public int getInterval() {
		return interval;
	}

	@Override
	public TimeRange getTimeRange() {
		return timeRange;
	}

	@Override
	public void setTimeRange(TimeRange timeRange) {
		this.timeRange = timeRange;

		for (LoggerEventListener callback : eventListeners) {
			try {
				callback.onSetTimeRange(this);
			} catch (Throwable t) {
				slog.warn("araqne log api; logger [" + getFullName() + "] callback should not throw any exception", t);
			}
		}
	}

	@Override
	public LoggerFactory getFactory() {
		return factory;
	}

	/**
	 * start passive logger
	 */
	@Override
	public void start(LoggerStartReason reason) {
		verifyPending();

		if (!isPassive())
			throw new IllegalStateException("not passive mode. use start(interval)");

		pending = false;
		stopped = false;
		invokeStartCallback(reason);
	}

	/**
	 * start active logger
	 */
	@Override
	public void start(LoggerStartReason reason, int interval) {
		verifyPending();

		if (isPassive()) {
			start(reason);
			return;
		}

		if (!stopped)
			throw new IllegalStateException("logger is already running");

		status = LoggerStatus.Starting;
		this.interval = interval;

		invokeStartCallback(reason);

		if (getExecutor() == null) {
			t = new Thread(this, "Logger [" + fullName + "]");
			t.start();
		} else {
			stopped = false;
			getExecutor().execute(this);
		}

		pending = false;
	}

	private void verifyPending() {
		if ((config.get("transformer") != null) && (transformer == null))
			throw new IllegalStateException("pending transformer");

		if (!unresolvedLoggers.isEmpty())
			throw new IllegalStateException("pending logger");
	}

	protected ExecutorService getExecutor() {
		return null;
	}

	private void invokeStartCallback(LoggerStartReason reason) {
		if (reason == LoggerStartReason.USER_REQUEST)
			enabled = true;

		lastStopReason = null;
		stopCallbacked = false;

		onStart(reason);

		lastStartDate = new Date();
		status = LoggerStatus.Running;

		for (LoggerEventListener callback : eventListeners) {
			try {
				callback.onStart(this);
			} catch (Throwable t) {
				slog.warn("araqne log api; logger [" + getFullName() + "] callback should not throw any exception", t);
			}
		}
	}

	@Override
	public void stop(LoggerStopReason reason) {
		if (lastStopReason == null)
			lastStopReason = reason;

		if (isPassive()) {
			invokeStopCallback(reason);
			stopped = true;
			status = LoggerStatus.Stopped;
			this.pending = ((reason == LoggerStopReason.TRANSFORMER_DEPENDENCY) || (reason == LoggerStopReason.LOGGER_DEPENDENCY));
		} else
			stop(reason, INFINITE);
	}

	@Override
	public void stop(LoggerStopReason reason, int maxWaitTime) {
		if (lastStopReason == null)
			lastStopReason = reason;

		if (isPassive()) {
			stop(reason);
			return;
		}

		status = LoggerStatus.Stopping;
		doStop = true;

		// e.g. close socket at onStop() can unblock waiting connect() call
		invokeStopCallback(reason);

		if (t != null) {
			if (!t.isAlive()) {
				t = null;
				return;
			}
			t.interrupt();
			t = null;
		}

		if (getExecutor() == null) {
			long begin = new Date().getTime();
			try {
				while (true) {
					if (stopped)
						break;

					if (maxWaitTime != 0 && new Date().getTime() - begin > maxWaitTime)
						break;

					Thread.sleep(50);
				}

				status = LoggerStatus.Stopped;
				stopped = true;
			} catch (InterruptedException e) {
			}
		} else {
			status = LoggerStatus.Stopped;
			stopped = true;
		}

		this.pending = pending;
	}

	/**
	 * called in explicit stop() call context
	 */
	private void invokeStopCallback(LoggerStopReason reason) {
		if (lastStopReason == null)
			lastStopReason = reason;

		if (stopCallbacked)
			return;

		stopCallbacked = true;

		if (reason == LoggerStopReason.USER_REQUEST) {
			enabled = false;
		}

		try {
			onStop(lastStopReason);
		} catch (Exception e) {
			slog.warn("araqne log api: [" + fullName + "] stop callback should not throw any exception", e);
		}

		for (LoggerEventListener callback : eventListeners) {
			try {
				callback.onStop(this, lastStopReason);
			} catch (Exception e) {
				slog.warn("logger callback should not throw any exception", e);
			}
		}
	}

	protected abstract void runOnce();

	// can be overridden
	protected void onStart(LoggerStartReason reason) {
	}

	// can be overridden
	protected void onStop(LoggerStopReason reason) {
	}

	// can be overridden
	protected void onResetStates() {
	}

	@Override
	public void run() {
		if (getExecutor() == null) {
			stopped = false;
			try {
				while (true) {
					try {
						if (doStop)
							break;

						if (timeRange != null && !timeRange.isInRange(new Date())) {
							if (slog.isDebugEnabled())
								slog.debug("araqne log api : skip logger [{}] run, waiting time range [{}]", getFullName(),
										timeRange);

							Thread.sleep(1000);
							continue;
						}

						long startedAt = System.currentTimeMillis();
						runOnce();
						if (doStop)
							break;

						long elapsed = System.currentTimeMillis() - startedAt;
						lastRunDate = new Date();
						if (interval - elapsed < 0)
							continue;
						Thread.sleep(interval - elapsed);
					} catch (InterruptedException e) {
					}
				}
			} catch (Throwable t) {
				slog.error("araqne log api: logger [" + getFullName() + "] stopped", t);
			} finally {
				status = LoggerStatus.Stopped;
				stopped = true;
				doStop = false;
			}
		} else {
			if (!enabled)
				return;

			if (lastRunDate != null) {
				long millis = lastRunDate.getTime() + (long) interval - System.currentTimeMillis();
				if (millis > 0) {
					try {
						Thread.sleep(Math.min(millis, 500));
					} catch (InterruptedException e) {
					}
					if (millis > 500) {
						ExecutorService executor = getExecutor();
						if (executor != null)
							executor.execute(this);
						return;
					}
				}
			}

			runOnce();
			lastRunDate = new Date();
			ExecutorService executor = getExecutor();
			if (executor != null)
				executor.execute(this);
		}
	}

	protected void writeBatch(Log[] logs) {
		// call method to support overriding (ex. base remote logger)
		if (!isRunning())
			return;

		int addCount = 0;
		int dropCount = 0;

		for (int i = 0; i < logs.length; i++) {
			Log log = logs[i];
			if (log == null)
				continue;

			// update last log date
			lastLogDate = log.getDate();
			lastLog = log;
			addCount++;

			// transform
			if (transformer != null)
				log = transformer.transform(log);

			logs[i] = log;

			// transform may return null to filter log
			if (log == null) {
				dropCount++;
				continue;
			}
		}

		if (addCount > dropCount)
			lastWriteDate = new Date();

		logCounter.addAndGet(addCount);
		dropCounter.addAndGet(dropCount);

		// notify all
		for (LogPipe pipe : pipes) {
			try {
				pipe.onLogBatch(this, logs);
			} catch (LoggerStopException e) {
				LoggerStopReason reason = LoggerStopReason.STOP_EXCEPTION;
				if (e.getCause() != null && e.getCause().getMessage() != null
						&& e.getCause().getMessage().contains("archive not opened"))
					reason = LoggerStopReason.LOW_DISK;

				this.slog.warn("araqne-log-api: stopping logger [" + getFullName() + "] by exception", e);
				if (isPassive())
					stop(reason);
				else {
					doStop = true;
					status = LoggerStatus.Stopping;
					invokeStopCallback(reason);
				}
			} catch (Throwable t) {
				if (t.getMessage() != null && t.getMessage().startsWith("invalid time"))
					this.slog.warn("araqne-log-api: log pipe should not throw exception" + t.getMessage());
				else
					this.slog.warn("araqne-log-api: log pipe should not throw exception", t);
			}
		}
	}

	protected void write(Log log) {
		// call method to support overriding (ex. base remote logger)
		if (!isRunning())
			return;

		// update last log date
		lastLogDate = log.getDate();
		lastLog = log;
		logCounter.incrementAndGet();

		// transform
		if (transformer != null)
			log = transformer.transform(log);

		// transform may return null to filter log
		if (log == null) {
			dropCounter.incrementAndGet();
			return;
		}

		lastWriteDate = new Date();

		// notify all
		for (LogPipe pipe : pipes) {
			try {
				pipe.onLog(this, log);
			} catch (LoggerStopException e) {
				this.slog.warn("araqne-log-api: stopping logger [" + getFullName() + "] by exception", e);
				if (isPassive())
					stop(LoggerStopReason.STOP_EXCEPTION);
				else {
					doStop = true;
					status = LoggerStatus.Stopping;
					invokeStopCallback(LoggerStopReason.STOP_EXCEPTION);
				}
			} catch (Exception e) {
				if (e.getMessage() != null && e.getMessage().startsWith("invalid time"))
					this.slog.warn("araqne-log-api: log pipe should not throw exception" + e.getMessage());
				else
					this.slog.warn("araqne-log-api: log pipe should not throw exception", e);
			}
		}
	}

	@Override
	public void updateConfigs(Map<String, String> configs) {
		if (!(this instanceof Reconfigurable))
			throw new UnsupportedOperationException("logger is not reconfigurable: " + getFullName());

		// clone old configs
		Map<String, String> oldConfigs = new HashMap<String, String>(this.config);
		Map<String, String> newConfigs = configs;

		// validate new configs
		for (LoggerConfigOption option : factory.getConfigOptions()) {
			if (!(option instanceof Mutable)) {
				String name = option.getName();
				String oldValue = oldConfigs.get(name);
				String newValue = newConfigs.get(name);
				if ((oldValue == null && newValue != null) || (oldValue != null && !oldValue.equals(newValue)))
					throw new IllegalArgumentException("option " + name + " is not mutable");
			}
		}

		this.config = configs;

		try {
			((Reconfigurable) this).onConfigChange(oldConfigs, newConfigs);
		} catch (Throwable t) {
			this.config = oldConfigs;
			throw new IllegalStateException(t);
		}

		notifyConfigChange();
	}

	private void notifyConfigChange() {
		for (LoggerEventListener callback : eventListeners) {
			try {
				callback.onUpdated(this, config);
			} catch (Exception e) {
				slog.error("araqne log api: logger event callback should not throw any exception", e);
			}
		}
	}

	/**
	 * Use getConfigs() instead
	 */
	@Deprecated
	@Override
	public Map<String, String> getConfig() {
		return new HashMap<String, String>(config);
	}

	@Override
	public Map<String, String> getConfigs() {
		return new HashMap<String, String>(config);
	}

	@Override
	public Map<String, Object> getStates() {
		LastStateService lastStateService = factory.getLastStateService();
		if (lastStateService == null)
			return new HashMap<String, Object>();

		LastState s = lastStateService.getState(getFullName());
		if (s == null)
			return new HashMap<String, Object>();

		return s.getProperties();
	}

	@Override
	public void setStates(Map<String, Object> state) {
		LastStateService lastStateService = factory.getLastStateService();
		if (lastStateService == null)
			throw new IllegalStateException("last status service not found");

		LastState s = new LastState();
		s.setLoggerName(getFullName());
		s.setInterval(interval);

		if (timeRange != null) {
			s.setStartTime(timeRange.getStartTime());
			s.setEndTime(timeRange.getEndTime());
		}

		s.setLogCount(logCounter.get());
		s.setDropCount(dropCounter.get());
		s.setLastLogDate(lastLogDate);
		s.setPending(pending);
		s.setEnabled(enabled);
		s.setRunning(status == LoggerStatus.Running);
		s.setProperties(state);

		lastStateService.setState(s);
		slog.trace("araqne log api: running state saved: {}", getFullName());
	}

	@Override
	public void resetStates() {
		logCounter.set(0);
		dropCounter.set(0);
		lastLogDate = null;
		setStates(new HashMap<String, Object>());

		try {
			onResetStates();
		} catch (Throwable t) {
			slog.warn("araqne log api: logger [" + getFullName() + "] throws exception at onResetStates()", t);
		}
	}

	@Override
	public void reloadStates() {
		LastStateService lss = factory.getLastStateService();
		LastState state = lss.getState(fullName);
		long lastLogCount = 0;
		long lastDropCount = 0;

		if (state != null) {
			this.interval = state.getInterval();
			this.enabled = state.isEnabled();

			if (state.getStartTime() != null && state.getEndTime() != null)
				this.timeRange = new TimeRange(state.getStartTime(), state.getEndTime());

			lastLogCount = state.getLogCount();
			lastDropCount = state.getDropCount();
			lastLogDate = state.getLastLogDate();
		}
		this.logCounter = new AtomicLong(lastLogCount);
		this.dropCounter = new AtomicLong(lastDropCount);
		this.lastLogDate = lastLogDate;
	}

	@Override
	public LogTransformer getTransformer() {
		return transformer;
	}

	@Override
	public void setTransformer(LogTransformer transformer) {
		this.transformer = transformer;

		boolean transformerResolved = (config.get("transformer") == null) || (transformer != null);
		if (!manualStart && enabled && isPending() && transformerResolved && unresolvedLoggers.isEmpty()) {
			start(LoggerStartReason.DEPENDENCY_RESOLVED, getInterval());
		}
		if ((enabled) && (!transformerResolved)) {
			stop(LoggerStopReason.TRANSFORMER_DEPENDENCY, 5000);
		}
	}

	@Override
	public void addLogPipe(LogPipe pipe) {
		if (pipe == null)
			throw new IllegalArgumentException("pipe should be not null");

		pipes.add(pipe);
	}

	@Override
	public void removeLogPipe(LogPipe pipe) {
		if (pipe == null)
			throw new IllegalArgumentException("pipe should be not null");

		pipes.remove(pipe);
	}

	@Override
	public void addEventListener(LoggerEventListener callback) {
		if (callback == null)
			throw new IllegalArgumentException("logger event listener must be not null");

		eventListeners.add(callback);
	}

	@Override
	public void removeEventListener(LoggerEventListener callback) {
		if (callback == null)
			throw new IllegalArgumentException("logger event listener must be not null");

		eventListeners.remove(callback);
	}

	@Override
	public void clearEventListeners() {
		eventListeners.clear();
	}

	public Set<String> getUnresolvedLoggers() {
		return new HashSet<String>(unresolvedLoggers);
	}

	@Override
	public boolean hasUnresolvedLoggers() {
		return !unresolvedLoggers.isEmpty();
	}

	@Override
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
		notifyConfigChange();
	}

	@Override
	public void setInterval(int interval) {
		this.interval = interval;
	}

	public void addUnresolvedLogger(String fullName) {
		if (this.slog.isDebugEnabled()) {
			this.slog.debug("araqne log api: logger [{}] has unresolved logger [{}]", getFullName(), fullName);
		}

		unresolvedLoggers.add(fullName);

		if (this.enabled) {
			stop(LoggerStopReason.LOGGER_DEPENDENCY, 5000);
		}
	}

	public void removeUnresolvedLogger(String fullName) {
		if (this.slog.isDebugEnabled()) {
			this.slog.debug("araqne log api: logger [{}] has resolved logger [{}]", getFullName(), fullName);
		}

		if (!this.unresolvedLoggers.remove(fullName))
			return;

		boolean transformerResolved = config.get("transformer") == null || transformer != null;
		if (!manualStart && status != LoggerStatus.Running && enabled && transformerResolved && unresolvedLoggers.isEmpty()) {
			start(LoggerStartReason.DEPENDENCY_RESOLVED, getInterval());
		}
	}

	@Override
	public String toString() {
		String format = "yyyy-MM-dd HH:mm:ss";
		String start = DateFormat.format(format, lastStartDate);
		String run = DateFormat.format(format, lastRunDate);
		String log = DateFormat.format(format, lastLogDate);
		String status = getStatus().toString().toLowerCase();
		if (isPassive())
			status += " (passive)";
		else
			status += " (interval=" + interval + "ms)";

		return String.format("name=%s, factory=%s, status=%s, log count=%d, last start=%s, last run=%s, last log=%s",
				getFullName(), factoryFullName, status, getLogCount(), start, run, log);
	}
}
