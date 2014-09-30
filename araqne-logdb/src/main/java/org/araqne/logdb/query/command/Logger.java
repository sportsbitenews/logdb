package org.araqne.logdb.query.command;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.LoggerRegistry;
import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;
import org.slf4j.LoggerFactory;

public class Logger extends DriverQueryCommand {
	private final org.slf4j.Logger logger = LoggerFactory.getLogger(Logger.class);

	private LoggerRegistry loggerRegistry;

	public Logger(LoggerRegistry loggerRegistry) {
		this.loggerRegistry = loggerRegistry;
	}

	@Override
	public String getName() {
		return "logger";
	}

	public void run() {
		try {
			for (org.araqne.log.api.Logger logger : loggerRegistry.getLoggers()) {
				Map<String, Object> m = new HashMap<String, Object>();
				m.put("namespace", logger.getNamespace());
				m.put("name", logger.getName());
				m.put("factory_namespace", logger.getFactoryNamespace());
				m.put("factory_name", logger.getFactoryName());
				m.put("status", logger.getStatus().toString());
				m.put("interval", logger.getInterval());
				m.put("log_count", logger.getLogCount());
				m.put("drop_count", logger.getDropCount());
				m.put("last_start_at", logger.getLastStartDate());
				m.put("last_run_at", logger.getLastRunDate());
				m.put("last_log_at", logger.getLastLogDate());
				m.put("last_write_at", logger.getLastWriteDate());
				pushPipe(new Row(m));
			}
		} catch (Throwable t) {
			logger.error("araqne logdb: failed to load logger status");
			Map<String, String> params = new HashMap<String, String>();
			params.put("msg", t.getMessage());
			throw new QueryParseException("60000", -1, -1, params);
		//	throw new QueryParseException("logger-load-fail", -1);
		}
	}
}
