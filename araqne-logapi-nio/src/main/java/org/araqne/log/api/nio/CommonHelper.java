package org.araqne.log.api.nio;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.log.api.LastPosition;
import org.araqne.log.api.ScanPeriodMatcher;

public class CommonHelper {

	private Pattern dirPathPattern;
	private Pattern fileNamePattern;
	private ScanPeriodMatcher scanPeriodMatcher;

	public CommonHelper(Pattern dirPathPattern, Pattern fileNamePattern, ScanPeriodMatcher scanPeriodMatcher) {
		this.dirPathPattern = dirPathPattern;
		this.fileNamePattern = fileNamePattern;
		this.scanPeriodMatcher = scanPeriodMatcher;
	}

	public boolean removeOutdatedStates(Map<String, LastPosition> lastPositions) {
		if (scanPeriodMatcher == null || lastPositions == null)
			return false;

		long now = System.currentTimeMillis();
		List<String> removeKeys = new ArrayList<String>();

		for (String path : lastPositions.keySet()) {
			String dateFromPath = getDateString(new File(path));
			if (dateFromPath == null)
				continue;

			if (!scanPeriodMatcher.matches(now, dateFromPath))
				removeKeys.add(path);
		}

		if (removeKeys.size() > 0) {
			for (String path : removeKeys)
				lastPositions.remove(path);

			return true;
		}
		
		return false;
	}

	public String getDateString(File f) {
		StringBuilder sb = new StringBuilder(f.getAbsolutePath().length());
		String dirPath = f.getParentFile().getAbsolutePath();
		if (dirPathPattern != null) {
			Matcher dirNameDateMatcher = dirPathPattern.matcher(dirPath);
			while (dirNameDateMatcher.find()) {
				int dirNameGroupCount = dirNameDateMatcher.groupCount();
				if (dirNameGroupCount > 0) {
					for (int i = 1; i <= dirNameGroupCount; ++i) {
						sb.append(dirNameDateMatcher.group(i));
					}
				}
			}
		}

		String fileName = f.getName();
		Matcher fileNameDateMatcher = fileNamePattern.matcher(fileName);
		while (fileNameDateMatcher.find()) {
			int fileNameGroupCount = fileNameDateMatcher.groupCount();
			if (fileNameGroupCount > 0) {
				for (int i = 1; i <= fileNameGroupCount; ++i) {
					sb.append(fileNameDateMatcher.group(i));
				}
			}
		}
		String date = sb.toString();
		return date.isEmpty() ? null : date;
	}

}
