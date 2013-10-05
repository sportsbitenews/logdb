package org.araqne.logstorage.file;

import java.io.File;
import java.io.IOException;

public interface LogFileRepairer {
	public LogFileFixReport quickFix(File indexPath, File dataPath) throws IOException;
	public LogFileFixReport fix(File indexPath, File dataPath) throws IOException;	
}
