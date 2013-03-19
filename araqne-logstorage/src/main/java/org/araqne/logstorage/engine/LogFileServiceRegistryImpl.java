package org.araqne.logstorage.engine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.logstorage.LogFileService;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.file.LogFileReader;
import org.araqne.logstorage.file.LogFileWriter;

@Component(name = "logstorage-log-file-service-registry")
@Provides
public class LogFileServiceRegistryImpl implements LogFileServiceRegistry {
	ConcurrentMap<String, LogFileService> services = new ConcurrentHashMap<String, LogFileService>();

	@Override
	public LogFileWriter newWriter(String type, Map<String, Object> options) throws Exception {
		LogFileService logFileService = services.get(type);
		return logFileService.newWriter(options);
	}

	@Override
	public LogFileReader newReader(String type, Map<String, Object> options) throws Exception {
		LogFileService logFileService = services.get(type);
		return logFileService.newReader(options);
	}

	@Override
	public void register(LogFileService service) {
		services.put(service.getType(), service);
	}

	@Override
	public void unregister(LogFileService service) {
		services.remove(service.getType());
	}

	@Override
	public String[] getServiceTypes() {
		return services.keySet().toArray(new String[0]);
	}

	@Override
	public LogFileService getLogFileService(String type) {
		return services.get(type);
	}

}
