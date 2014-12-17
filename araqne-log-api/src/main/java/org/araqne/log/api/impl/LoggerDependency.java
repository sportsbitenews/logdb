package org.araqne.log.api.impl;

import java.util.HashMap;
import java.util.Map;

import org.araqne.confdb.CollectionName;

// do not implement msgbus.Marshalable. sentry should not require msgbus bundle.
@CollectionName("logger_dependencies")
public class LoggerDependency {
	private String logger;
	private String source;

	public LoggerDependency() {
	}

	public LoggerDependency(String logger, String source) {
		this.logger = logger;
		this.source = source;
	}

	public String getLogger() {
		return logger;
	}

	public void setLogger(String logger) {
		this.logger = logger;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public Map<String, Object> marshal() {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("logger", logger);
		m.put("source", source);
		return m;
	}
}
