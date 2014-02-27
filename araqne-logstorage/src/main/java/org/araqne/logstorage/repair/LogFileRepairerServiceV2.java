package org.araqne.logstorage.repair;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logstorage.LogFileRepairer;
import org.araqne.logstorage.LogFileRepairerRegistry;
import org.araqne.logstorage.LogFileRepairerService;
import org.araqne.logstorage.file.LogFileRepairerV2;

@Component(name = "logstorage-log-file-repairer-v2")
public class LogFileRepairerServiceV2 implements LogFileRepairerService {
	@Requires
	private LogFileRepairerRegistry registry;

	@Override
	public String getType() {
		return "v2";
	}

	@Override
	public LogFileRepairer newRepairer() {
		return new LogFileRepairerV2();
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
