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
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.LogParser;
import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextFile extends DriverQueryCommand {
	private final Logger logger = LoggerFactory.getLogger(TextFile.class.getName());
	private String filePath;
	private LogParser parser;
	private int offset;
	private int limit;

	public TextFile(String filePath, LogParser parser, int offset, int limit) {
		this.filePath = filePath;
		this.parser = parser;
		this.offset = offset;
		this.limit = limit;
	}

	@Override
	public void run() {
		status = Status.Running;

		FileInputStream is = null;
		BufferedReader br = null;
		try {
			Charset utf8 = Charset.forName("utf-8");
			is = new FileInputStream(new File(filePath));
			br = new BufferedReader(new InputStreamReader(new BufferedInputStream(is), utf8));

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
					pushPipe(new Row(parsed != null ? parsed : m));
					count++;
				}
				i++;
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
		return "textfile offset=" + offset + " limit=" + limit + " " + filePath;
	}
}
