package org.araqne.logstorage;

public interface LogFileRepairerService {
	public String getType();
	public LogFileRepairer newRepairer();
}
