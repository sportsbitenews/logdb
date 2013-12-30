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
package org.araqne.logdb.jython;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;

import org.araqne.api.DateFormat;
import org.araqne.log.api.LastState;
import org.araqne.log.api.LastStateService;
import org.araqne.log.api.Log;
import org.araqne.log.api.LogPipe;
import org.araqne.log.api.LogTransformer;
import org.araqne.log.api.Logger;
import org.araqne.log.api.LoggerEventListener;
import org.araqne.log.api.LoggerFactory;
import org.araqne.log.api.LoggerRegistry;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.LoggerStatus;

public abstract class JythonActiveLogger implements Logger, Runnable {
	private final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(JythonActiveLogger.class.getName());

	private LoggerFactory factory;
	private LoggerSpecification spec;
	private LogTransformer transformer;
	private CopyOnWriteArraySet<LogPipe> pipes = new CopyOnWriteArraySet<LogPipe>();
	private CopyOnWriteArraySet<LoggerEventListener> listeners = new CopyOnWriteArraySet<LoggerEventListener>();

	private Thread t;
	private int interval;
	private Map<String, String> config;

	private volatile LoggerStatus status = LoggerStatus.Stopped;
	private volatile boolean doStop = false;
	private volatile boolean stopped = true;
	private volatile boolean pending;
	private volatile boolean manualStart;
	private volatile Date lastStartDate;
	private volatile Date lastRunDate;
	private volatile Date lastLogDate;
	private AtomicLong logCounter = new AtomicLong();
	private AtomicLong dropCounter = new AtomicLong();

	public abstract void init(LoggerSpecification spec);

	protected abstract void runOnce();

	protected void onStop() {
	}

	public void preInit(LoggerFactory factory, LoggerSpecification spec) {
		this.factory = factory;
		this.spec = spec;
		this.config = spec.getConfig();
	}

	@Override
	public void run() {
		stopped = false;
		try {
			while (true) {
				try {
					if (doStop)
						break;
					long startedAt = System.currentTimeMillis();
					runOnce();
					updateConfig(config);
					long elapsed = System.currentTimeMillis() - startedAt;
					lastRunDate = new Date();
					if (interval - elapsed < 0)
						continue;
					Thread.sleep(interval - elapsed);
				} catch (InterruptedException e) {
				}
			}
		} catch (Exception e) {
			log.error("araqne log api: logger stopped", e);
		} finally {
			status = LoggerStatus.Stopped;
			stopped = true;
			doStop = false;

			try {
				onStop();
			} catch (Exception e) {
				log.warn("araqne log api: [" + getFullName() + "] stop callback should not throw any exception", e);
			}
		}
	}

	@Override
	public String getFullName() {
		return spec.getNamespace() + "\\" + spec.getName();
	}

	@Override
	public String getNamespace() {
		return spec.getNamespace();
	}

	@Override
	public String getName() {
		return spec.getName();
	}

	@Override
	public String getFactoryFullName() {
		return factory.getFullName();
	}

	@Override
	public String getFactoryName() {
		return factory.getName();
	}

	@Override
	public String getFactoryNamespace() {
		return factory.getNamespace();
	}

	@Override
	public String getDescription() {
		return spec.getDescription();
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
	public long getLogCount() {
		return logCounter.get();
	}

	@Override
	public boolean isRunning() {
		return !stopped;
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
	public void start() {
		throw new UnsupportedOperationException("this is active logger, start with interval");
	}

	@Override
	public void start(int interval) {
		if (!stopped)
			throw new IllegalStateException("logger is already running");

		status = LoggerStatus.Starting;
		this.interval = interval;

		t = new Thread(this, "Logger [" + getFullName() + "]");
		t.start();

		invokeStartCallback();
	}

	@Override
	public void stop() {
		stop(0);
	}

	@Override
	public void stop(int maxWaitTime) {
		if (t != null) {
			if (!t.isAlive()) {
				t = null;
				return;
			}
			t.interrupt();
			t = null;
		}

		status = LoggerStatus.Stopping;

		doStop = true;
		long begin = new Date().getTime();
		try {
			while (true) {
				if (stopped)
					break;

				if (maxWaitTime != 0 && new Date().getTime() - begin > maxWaitTime)
					break;

				Thread.sleep(50);
			}
		} catch (InterruptedException e) {
		}

		invokeStopCallback();
	}

	@Override
	public void addLogPipe(LogPipe pipe) {
		pipes.add(pipe);
	}

	@Override
	public void removeLogPipe(LogPipe pipe) {
		pipes.remove(pipe);
	}

	@Override
	public void addEventListener(LoggerEventListener callback) {
		listeners.add(callback);
	}

	@Override
	public void removeEventListener(LoggerEventListener callback) {
		listeners.remove(callback);
	}

	@Override
	public void clearEventListeners() {
		listeners.clear();
	}

	protected void write(Log log) {
		if (stopped)
			return;

		// update last log date
		lastLogDate = log.getDate();
		logCounter.incrementAndGet();

		// transform
		if (transformer != null) {
			log = transformer.transform(log);
		}

		if (log == null) {
			dropCounter.incrementAndGet();
			return;
		}

		// notify all
		for (LogPipe pipe : pipes) {
			try {
				pipe.onLog(this, log);
			} catch (Exception e) {
				if (e.getMessage() != null && e.getMessage().startsWith("invalid time"))
					this.log.warn("araqne logdb jython: log pipe should not throw exception" + e.getMessage());
				else
					this.log.warn("araqne logdb jython: log pipe should not throw exception", e);
			}
		}
	}

	@Override
	public void updateConfig(Map<String, String> config) {
		for (LoggerEventListener callback : listeners) {
			try {
				callback.onUpdated(this, config);
			} catch (Exception e) {
				log.error("araqne log api: logger event callback should not throw any exception", e);
			}
		}
	}

	@Override
	public Map<String, String> getConfig() {
		return config;
	}

	private void invokeStartCallback() {
		lastStartDate = new Date();
		status = LoggerStatus.Running;

		setState(getState());

		for (LoggerEventListener callback : listeners) {
			try {
				callback.onStart(this);
			} catch (Exception e) {
				log.warn("logger callback should not throw any exception", e);
			}
		}
	}

	private void invokeStopCallback() {
		LastState s = new LastState();
		s.setLoggerName(getFullName());
		s.setLogCount(getLogCount());
		s.setDropCount(getDropCount());
		s.setLastLogDate(getLastLogDate());
		s.setPending(isPending());
		s.setProperties(getState());
		s.setRunning(isRunning());

		getFactory().getLastStateService().setState(s);

		for (LoggerEventListener callback : listeners) {
			try {
				callback.onStop(this);
			} catch (Exception e) {
				log.warn("logger callback should not throw any exception", e);
			}
		}
	}

	@Override
	public long getDropCount() {
		return dropCounter.get();
	}

	@Override
	public boolean isPending() {
		return pending;
	}

	@Override
	public void setPending(boolean pending) {
		this.pending = pending;
	}

	@Override
	public boolean isManualStart() {
		return manualStart;
	}

	@Override
	public void setManualStart(boolean manualStart) {
		this.manualStart = manualStart;
	}

	@Override
	public LogTransformer getTransformer() {
		return transformer;
	}

	@Override
	public void setTransformer(LogTransformer transformer) {
		this.transformer = transformer;
	}

	@Override
	public String toString() {
		String format = "yyyy-MM-dd HH:mm:ss";
		String start = DateFormat.format(format, lastStartDate);
		String run = DateFormat.format(format, lastRunDate);
		String log = DateFormat.format(format, lastLogDate);
		String status = getStatus().toString().toLowerCase();
		status += " (interval=" + interval + "ms)";

		return String.format("name=%s, factory=%s, script=%s, status=%s, log count=%d, last start=%s, last run=%s, last log=%s",
				getFullName(), factory.getFullName(), getClass().getSimpleName(), status, getLogCount(), start, run, log);
	}

}
