package org.araqne.logstorage;

public interface LogFileRepairerRegistry {
	void register(LogFileRepairerService repairer);

	void unregister(LogFileRepairerService repairer);

	void uninstall(String type);

	String[] getRepairerTypes();

	String[] getInstalledTypes();

	LogFileRepairer newRepairer(String type);
}
