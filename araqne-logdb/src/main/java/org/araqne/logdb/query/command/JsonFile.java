/*
 * Copyright 2011 Future Systems
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
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.araqne.log.api.LogParser;
import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.Row;
import org.json.JSONConverter;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonFile extends DriverQueryCommand {
	private final Logger logger = LoggerFactory.getLogger(JsonFile.class.getName());
	private List<String> filePaths;
	private String filePath;
	private LogParser parser;
	private String parseTarget;
	private long offset;
	private long limit;
	private boolean overlay;

	public JsonFile(List<String> filePaths, String filePath, LogParser parser, String parseTarget, boolean overlay, long offset,
			long limit) {
		this.filePaths = filePaths;
		this.filePath = filePath;
		this.parser = parser;
		this.parseTarget = parseTarget;
		this.overlay = overlay;
		this.offset = offset;
		this.limit = limit;

		if (this.parseTarget == null) {
			this.parseTarget = "line";
		}
	}

	@Override
	public String getName() {
		return "jsonfile";
	}

	@Override
	public void run() {
		status = Status.Running;

		for (String filePath : filePaths)
			readJsonFile(filePath);
	}

	private void readJsonFile(String filePath) {
		FileInputStream is = null;
		BufferedReader br = null;
		try {
			Charset utf8 = Charset.forName("utf-8");
			File f = new File(filePath);
			is = new FileInputStream(f);
			br = new BufferedReader(new InputStreamReader(new BufferedInputStream(is), utf8));

			int i = 0;
			int count = 0;
			while (true) {
				if (limit > 0 && count >= limit)
					break;

				String line = br.readLine();
				if (line == null)
					break;

				JSONTokener tokenizer = new JSONTokener(new StringReader(line));
				Object value = tokenizer.nextValue();

				if (value instanceof JSONObject) {
					Map<String, Object> m = JSONConverter.parse((JSONObject) value);
					if (parser != null && m.containsKey(parseTarget)) {
						Map<String, Object> parsed = parser.parse(m);
						if (parsed != null)
							if (overlay) {
								for (Map.Entry<String, Object> e : parsed.entrySet()) {
									m.put(e.getKey(), e.getValue());
								}
							} else {
								m = parsed;
							}
					}

					if (i >= offset) {
						m.put("_file", f.getName());

						pushPipe(new Row(m));
						count++;
					}
					i++;
				} else {
					logger.warn("invalid json object in line: " + line);
					continue;
				}
			}
		} catch (Throwable t) {
			logger.error("araqne logdb: file error", t);
		} finally {
			IoHelper.close(br);
			IoHelper.close(is);
		}
	}

	@Override
	public String toString() {
		String offsetOpt = "";
		if (offset != 0)
			offsetOpt = " offset=" + offset;

		String limitOpt = "";
		if (limit != 0)
			limitOpt = " limit" + limit;

		return "jsonfile" + offsetOpt + limitOpt + " " + filePath;
	}
}
