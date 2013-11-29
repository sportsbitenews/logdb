/*
 * Copyright 2013 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.log.api;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LastPositionHelper {
	private LastPositionHelper() {
	}

	public static Map<String, LastPosition> readLastPositions(File f) {
		List<String> lines = readAllLine(f);
		return readLastPosition(lines);
	}

	public static Map<String, LastPosition> readLastPosition(List<String> lines) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH:mm:ss");
		Map<String, LastPosition> lastPositions = new HashMap<String, LastPosition>();
		if (lines == null || lines.isEmpty())
			return lastPositions;

		int startIndex = 0;
		int endIndex = lines.size() - 1;

		int version = 1;
		if (lines.get(0).equals("ARAQNE_LAST_POS_VER2")) {
			version = 2;
			startIndex++;
			endIndex--;
		}

		for (; startIndex <= endIndex; startIndex++) {
			String line = lines.get(startIndex);
			LastPosition inform = parseLine(version, line, sdf);
			if (inform == null)
				continue;

			File file = new File(inform.getPath());
			if (inform.getLastSeen() == null && !file.exists())
				inform.setLastSeen(new Date());
			if (inform.getLastSeen() != null && file.exists())
				inform.setLastSeen(null);
			lastPositions.put(inform.getPath(), inform);
		}

		return lastPositions;
	}

	public static List<String> parseV2Lines(Map<String, LastPosition> lastPositions) {
		List<String> lines = new ArrayList<String>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH:mm:ss");
		lines.add("ARAQNE_LAST_POS_VER2");
		long currentTime = new Date().getTime();

		for (String path : lastPositions.keySet()) {
			LastPosition inform = lastPositions.get(path);
			String position = Long.toString(inform.getPosition());
			String line = path + " " + position;
			if (inform.getLastSeen() != null) {
				long limitTime = inform.getLastSeen().getTime() + 3600000L;
				if (limitTime <= currentTime)
					continue;
				line += " " + sdf.format(inform.getLastSeen());
			} else
				line += " -";
			lines.add(line);
		}
		lines.add("END_FILE");
		return lines;
	}

	public static void updateLastPositionFile(File f, Map<String, LastPosition> lastPositions) {
		Logger logger = LoggerFactory.getLogger(LastPositionHelper.class);

		// write last positions
		FileOutputStream os = null;
		try {
			os = new FileOutputStream(f);
			List<String> lines = parseV2Lines(lastPositions);
			for (String line : lines) {
				if (!line.equals("END_FILE"))
					line += "\n";
				os.write(line.getBytes("utf-8"));
			}
		} catch (IOException e) {
			logger.error("araqne log api: cannot write last position file", e);
		} finally {
			if (os != null) {
				try {
					os.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private static List<String> readAllLine(File f) {
		Logger logger = LoggerFactory.getLogger(LastPositionHelper.class);
		try {
			if (f.exists()) {
				List<String> lines = new ArrayList<String>();
				FileInputStream is = null;
				BufferedReader br = null;
				try {
					is = new FileInputStream(f);
					br = new BufferedReader(new InputStreamReader(is, "utf-8"));
					while (true) {
						String line = br.readLine();
						if (line == null)
							break;
						if (line.trim().isEmpty())
							continue;

						lines.add(line.trim());
					}
				} finally {
					ensureClose(is);
					ensureClose(br);
				}
				return lines;
			}
		} catch (IOException e) {
			logger.error("araqne log api: apache logger cannot read last positions", e);
		}
		return null;
	}

	private static LastPosition parseLine(int version, String line, SimpleDateFormat sdf) {
		String path = null;
		long pos = 0;
		Date lastSeen = null;
		String posString = null;
		if (version == 1) {
			int p = line.lastIndexOf(" ", line.length());
			path = line.substring(0, p);
			posString = line.substring(p + 1);
			pos = posString.trim().isEmpty() ? 0 : Long.parseLong(posString);
			return new LastPosition(path, pos, null);
		} else if (version == 2) {
			int startDate = line.lastIndexOf(" ", line.length());
			String date = line.substring(startDate + 1);
			if (!date.equals("-")) {
				try {
					lastSeen = sdf.parse(line.substring(startDate + 1));
				} catch (ParseException e) {
				}
			}
			int startPos = line.lastIndexOf(" ", startDate - 1);
			pos = Long.parseLong(line.substring(startPos + 1, startDate));
			path = line.substring(0, startPos);
			return new LastPosition(path, pos, lastSeen);
		}
		return null;
	}

	private static void ensureClose(Closeable c) {
		try {
			if (c != null)
				c.close();
		} catch (IOException e) {
		}
	}
}
