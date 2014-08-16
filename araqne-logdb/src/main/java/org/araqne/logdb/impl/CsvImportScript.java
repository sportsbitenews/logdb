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
package org.araqne.logdb.impl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.araqne.api.Script;
import org.araqne.api.ScriptArgument;
import org.araqne.api.ScriptContext;
import org.araqne.api.ScriptUsage;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogBatchPacker;
import org.araqne.logstorage.LogCallback;
import org.araqne.logstorage.LogStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

public class CsvImportScript implements Script {
	private final Logger logger = LoggerFactory.getLogger(CsvImportScript.class);
	private final LogStorage storage;
	private ScriptContext context;

	public CsvImportScript(LogStorage storage) {
		this.storage = storage;
	}

	@Override
	public void setScriptContext(ScriptContext context) {
		this.context = context;
	}

	@ScriptUsage(description = "import text log file", arguments = {
			@ScriptArgument(name = "table name", type = "string", description = "table name"),
			@ScriptArgument(name = "file path", type = "string", description = "text log file path"),
			@ScriptArgument(name = "parse date", type = "string", description = "true or false", optional = true),
			@ScriptArgument(name = "charset", type = "string", description = "utf8 by default", optional = true), })
	public void importCsvFile(String[] args) throws InterruptedException {

		String table = args[0];
		String path = args[1];
		String charset = "utf-8";

		String[] dateColumns = null;
		SimpleDateFormat dateFormat = null;
		if (args.length > 2 && Boolean.parseBoolean(args[2])) {
			context.print("date columns? ");
			String cols = context.readLine();
			dateColumns = cols.split(",");
			for (int i = 0; i < dateColumns.length; i++)
				dateColumns[i] = dateColumns[i].trim();

			context.print("date format? ");
			String format = context.readLine().trim();
			context.print("date locale? ");
			String locale = context.readLine().trim();
			if (locale.isEmpty())
				locale = "en";

			dateFormat = new SimpleDateFormat(format, new Locale(locale));
		}

		if (args.length > 3)
			charset = args[3];

		long begin = System.currentTimeMillis();
		FileInputStream fis = null;
		InputStreamReader isr = null;
		CSVReader reader = null;
		String[] headers = null;
		LogBatchPacker packer = null;

		long count = 0;
		try {
			fis = new FileInputStream(path);
			isr = new InputStreamReader(fis, charset);
			reader = new CSVReader(isr);
			packer = new LogBatchPacker(table, 2000, 100, new LogCallback() {
				@Override
				public void onLogBatch(String tableName, List<Log> logBatch) {
					try {
						storage.write(logBatch);
					} catch (InterruptedException e) {
					}
				}
			});

			while (true) {
				String[] line = reader.readNext();
				if (line == null)
					break;

				if (headers == null) {
					headers = line;
					continue;
				}

				Map<String, Object> data = new HashMap<String, Object>();
				for (int i = 0; i < line.length; i++) {
					data.put(headers[i], line[i]);
				}

				Date date = parseDate(data, dateColumns, dateFormat);
				Log log = new Log(table, date, data);
				packer.pack(log);
				count++;
			}

			long end = System.currentTimeMillis();
			long elapsed = end - begin;
			long speed = count * 1000 / elapsed;

			packer.flush();
			context.println("loaded " + count + " logs in " + elapsed + "ms, " + speed + "logs/sec");
		} catch (IOException e) {
			context.println("failed to load csv -  " + e.getMessage());
			logger.error("araqne logdb: cannot load csv file", e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
				}
			}

			if (isr != null) {
				try {
					isr.close();
				} catch (IOException e) {
				}
			}

			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
				}
			}

			packer.close();
		}
	}

	private Date parseDate(Map<String, Object> data, String[] dateColumns, SimpleDateFormat df) {
		if (dateColumns == null || df == null)
			return new Date();

		String s = "";
		for (String c : dateColumns) {
			Object v = data.get(c);
			if (v != null)
				s += v;
		}

		Date d = df.parse(s, new ParsePosition(0));
		if (d == null)
			return new Date();
		return d;
	}
}
