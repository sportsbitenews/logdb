/*
 * Copyright 2013 Future Systems
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
package org.araqne.logdb.query.command;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;

import org.araqne.log.api.LogParser;
import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.ZipEntryPathHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipFile extends DriverQueryCommand {
	private final Logger slog = LoggerFactory.getLogger(ZipFile.class.getName());
	private List<String> filePaths;
	private String filePath;
	private String entryPath;
	private LogParser parser;
	private int offset;
	private int limit;

	public ZipFile(List<String> filePaths, String filePath, String entryPath, LogParser parser, int offset, int limit) {
		this.filePaths = filePaths;
		this.filePath = filePath;
		this.entryPath = entryPath;
		this.parser = parser;
		this.offset = offset;
		this.limit = limit;
	}

	@Override
	public String getName() {
		return "zipfile";
	}

	@Override
	public void run() {
		status = Status.Running;

		for (String filePath : filePaths) {

			readZipFile(filePath);
		}
	}

	private void readZipFile(String filePath) {
		java.util.zip.ZipFile zipFile = null;
		try {
			File f = new File(filePath);
			zipFile = new java.util.zip.ZipFile(f);
			ZipEntryPathHelper helper = new ZipEntryPathHelper(zipFile, this.entryPath);
			for (String entryPath : helper.getMatchedPaths())
				readZipEntry(filePath, zipFile, f, entryPath);
		} catch (Throwable t) {
			slog.error("araqne logdb: zipfile error", t);
		} finally {
			try {
				if (zipFile != null)
					zipFile.close();
			} catch (IOException e) {
			}
		}
	}

	private void readZipEntry(String filePath, java.util.zip.ZipFile zipFile, File f, String entryPath) {
		BufferedReader br = null;
		InputStream is = null;
		try {
			slog.debug("araqne logdb: zipfile path: {}, zip entry: {}", filePath, entryPath);

			ZipEntry entry = zipFile.getEntry(entryPath);
			if (entry == null)
				throw new IllegalStateException("entry [" + entryPath + "] not found in zip file [" + filePath + "]");

			is = zipFile.getInputStream(entry);
			br = new BufferedReader(new InputStreamReader(new BufferedInputStream(is), "utf-8"));

			int i = 0;
			int count = 0;
			while (true) {
				if (limit > 0 && count >= limit)
					break;

				String line = br.readLine();
				if (line == null)
					break;

				Map<String, Object> m = new HashMap<String, Object>();
				Map<String, Object> parsed = null;
				m.put("line", line);
				if (parser != null) {
					parsed = parser.parse(m);
					if (parsed == null)
						continue;
				}

				if (i >= offset) {
					Row r = new Row(parsed != null ? parsed : m);
					r.put("_file", f.getName());
					r.put("_entry", entry.getName());
					pushPipe(r);
					count++;
				}
				i++;
			}
		} catch (Throwable t) {
			slog.error("araqne logdb: zipfile error", t);
		} finally {
			IoHelper.close(br);
			IoHelper.close(is);
		}
	}

	@Override
	public String toString() {
		String offsetOpt = "";
		if (offset > 0)
			offsetOpt = " offset=" + offset;

		String limitOpt = "";
		if (limit > 0)
			limitOpt = " limit=" + limit;

		return "zipfile" + offsetOpt + limitOpt + " " + filePath + " " + entryPath;
	}
}
