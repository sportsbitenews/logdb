/**
 * Copyright 2015 Eediom Inc.
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
package org.araqne.logdb.xlsx;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.araqne.log.api.DummyLogger;
import org.araqne.log.api.Log;
import org.araqne.log.api.LogPipe;
import org.araqne.log.api.Logger;
import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.xml.sax.SAXException;

public class XlsxFileQuery extends DriverQueryCommand {
	private DummyLogger dummyLogger = new DummyLogger();
	private File file;
	private String path;
	private String sheetNameFilter;
	private long offset;
	private long limit;
	private long skip;

	private XlsxExtractor currentExtractor;
	private volatile boolean cancelled;

	public XlsxFileQuery(String path, String sheetNameFilter, long offset, long limit, long skip) {
		this.path = path;
		this.sheetNameFilter = sheetNameFilter;
		this.file = new File(path);
		this.offset = offset;
		this.limit = limit;
		this.skip = skip;
	}

	@Override
	public String getName() {
		return "xlsxfile";
	}

	@Override
	public void run() {
		List<String> sheetNames;
		try {
			sheetNames = XlsxExtractor.getSheetNames(file);
		} catch (InvalidFormatException e) {
			throw new IllegalStateException("invalid xlsx file: " + file.getAbsolutePath(), e);
		} catch (IOException e) {
			throw new IllegalStateException("cannot open xlsx file: " + file.getAbsolutePath(), e);
		}

		long nextOffset = offset;
		long nextLimit = limit;
		for (String sheetName : sheetNames) {
			try {
				if (cancelled)
					break;

				if (sheetNameFilter != null && !sheetNameFilter.equals(sheetName))
					continue;

				Pipe pipe = new Pipe(sheetName);
				currentExtractor = new XlsxExtractor(file, sheetName, dummyLogger, pipe, nextOffset, nextLimit, skip);
				currentExtractor.run();

				nextOffset -= currentExtractor.getTotalCount();
				if (nextOffset < 0)
					nextOffset = 0;

				nextLimit -= currentExtractor.getOutputCount();
				if (nextLimit <= 0)
					break;

			} catch (SAXException e) {
			} catch (Throwable t) {
				throw new IllegalStateException("xlsxfile query error", t);
			}
		}
	}

	@Override
	protected void onClose(QueryStopReason reason) {
		if (reason != QueryStopReason.End) {
			cancelled = true;
			currentExtractor.cancel();
		}
	}

	private class Pipe implements LogPipe {
		private String sheetName;

		public Pipe(String sheetName) {
			this.sheetName = sheetName;
		}

		@Override
		public void onLog(Logger logger, Log log) {
			Row row = new Row(log.getParams());
			row.put("_sheet", sheetName);
			pushPipe(row);
		}

		@Override
		public void onLogBatch(Logger logger, Log[] logs) {
		}
	}

	@Override
	public String toString() {
		String sheetOpt = "";
		if (sheetNameFilter != null)
			sheetOpt += " sheet=\"" + sheetNameFilter + "\"";

		String offsetOpt = "";
		if (offset > 0)
			offsetOpt = " offset=" + offset;

		String limitOpt = "";
		if (limit > 0)
			limitOpt = " limit=" + limit;

		String skipOpt = "";
		if (skip > 0)
			skipOpt = " skip=" + skip;

		return "xlsxfile" + sheetOpt + offsetOpt + limitOpt + skipOpt + " " + path;
	}
}
