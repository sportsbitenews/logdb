package org.araqne.logstorage.repair;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logstorage.LogFileRepairer;
import org.araqne.logstorage.LogFileRepairerRegistry;
import org.araqne.logstorage.LogFileRepairerService;

@Component(name = "logstorage-log-file-repairer-v1")
public class LogFileRepairerServiceV1 implements LogFileRepairerService {
	@Requires
	private LogFileRepairerRegistry registry;

	@Override
	public String getType() {
		return "v1";
	}

	@Override
	public LogFileRepairer newRepairer() {
		return null;
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
