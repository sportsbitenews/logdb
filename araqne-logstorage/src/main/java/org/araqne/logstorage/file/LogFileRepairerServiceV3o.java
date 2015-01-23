package org.araqne.logstorage.file;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logstorage.LogFileRepairer;
import org.araqne.logstorage.LogFileRepairerRegistry;
import org.araqne.logstorage.LogFileRepairerService;

@Component(name = "logstorage-log-file-repairer-v3o")
public class LogFileRepairerServiceV3o implements LogFileRepairerService {
	@Requires
	private LogFileRepairerRegistry registry;

	@Override
	public String getType() {
		return "v3o";
	}

	@Override
	public LogFileRepairer newRepairer() {
		return new LogFileRepairerV3o();
	}

	@Validate
	public void start() {
		registry.register(this);
	}

	@Invalidate
	public void stop() {
		if (registry != null)
			registry.unregister(this);
	}
}
