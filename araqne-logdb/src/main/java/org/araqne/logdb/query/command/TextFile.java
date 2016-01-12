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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.araqne.log.api.DummyLogger;
import org.araqne.log.api.Log;
import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogPipe;
import org.araqne.log.api.Logger;
import org.araqne.log.api.LoggerStatus;
import org.araqne.log.api.MultilineLogExtractor;
import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.QueryResultClosedException;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;

public class TextFile extends DriverQueryCommand {
	private List<String> filePaths;
	private String filePath;
	private LogParser parser;
	private long offset;
	private long limit;
	private String beginRegex;
	private String endRegex;
	private String dateFormat;
	private String datePattern;
	private String charset;
	private String fileTag;
	private long currentOffset;
	private long pushCount;
	private DummyLogger dummyLogger = new DummyLogger();

	public TextFile(List<String> filePaths, String filePath, LogParser parser, long offset, long limit, String beginRegex,
			String endRegex, String dateFormat, String datePattern, String charset, String fileTag) {
		this.filePaths = filePaths;
		this.filePath = filePath;
		this.parser = parser;
		this.offset = offset;
		this.limit = limit;
		this.beginRegex = beginRegex;
		this.endRegex = endRegex;
		this.dateFormat = dateFormat;
		this.datePattern = datePattern;
		this.charset = charset;
		this.fileTag = fileTag;
		currentOffset = 0;
		pushCount = 0;
	}

	@Override
	public void onClose(QueryStopReason reason) {
		dummyLogger.setStatus(LoggerStatus.Stopped);
	}

	@Override
	public String getName() {
		return "textfile";
	}

	private static class LimitReachedException extends RuntimeException {
		private static final long serialVersionUID = 1L;
	}

	private class RowPipe implements LogPipe {
		private String filePath;

		public RowPipe(String filePath) {
			this.filePath = filePath;
		}

		@Override
		public void onLog(Logger logger, Log log) {
			Row row = buildRow(log);
			pushRow(row);
		}

		private void pushRow(Row row) {
			if (limit > 0 && pushCount >= limit) {
				throw new LimitReachedException();
			}

			Map<String, Object> parsed = null;
			if (parser != null) {
				parsed = parser.parse(row.map());
				if (parsed == null)
					return;
			}

			if (currentOffset >= offset) {
				Row r = new Row(parsed != null ? parsed : row.map());
				if (fileTag != null)
					r.map().put(fileTag, filePath);
				pushPipe(r);
				pushCount++;
			}
			currentOffset++;
		}

		private void pushRows(List<Row> rows) {
			for (Row row : rows) {
				if (limit > 0 && pushCount >= limit) {
					throw new LimitReachedException();
				}

				Map<String, Object> parsed = null;
				if (parser != null) {
					parsed = parser.parse(row.map());
					if (parsed == null)
						return;
				}

				if (currentOffset >= offset) {
					Row r = new Row(parsed != null ? parsed : row.map());
					if (fileTag != null)
						r.map().put(fileTag, filePath);
					pushPipe(r);
					pushCount++;
				}
				currentOffset++;
			}
		}

		private Row buildRow(Log log) {
			Row row = new Row();
			row.put("_time", log.getDate());
			String line = (String) log.getParams().get("line");
			row.put("line", line);
			return row;
		}

		@Override
		public void onLogBatch(Logger logger, Log[] logs) {
			if (logs.length <= 0)
				return;

			List<Row> rows = new ArrayList<Row>(logs.length);
			for (Log log : logs) {
				if (log == null)
					continue;

				rows.add(buildRow(log));
			}

			pushRows(rows);
		}
	}

	@Override
	public void run() {
		status = Status.Running;

		for (String filePath : filePaths) {
			FileInputStream is = null;
			BufferedReader br = null;
			try {
				RowPipe pipe = new RowPipe(filePath);
				is = new FileInputStream(new File(filePath));

				MultilineLogExtractor extractor = new MultilineLogExtractor(dummyLogger, pipe);
				if (beginRegex != null)
					extractor.setBeginMatcher(Pattern.compile(beginRegex).matcher(""));
				if (endRegex != null)
					extractor.setEndMatcher(Pattern.compile(endRegex).matcher(""));
				if (datePattern != null)
					extractor.setDateMatcher(Pattern.compile(datePattern).matcher(""));
				if (dateFormat != null)
					extractor.setDateFormat(new SimpleDateFormat(dateFormat));
				extractor.setCharset(Charset.forName(charset).name());
				extractor.extract(is, new AtomicLong());
			} catch (LimitReachedException e) {
				// ignore
			} catch (QueryResultClosedException e) {
				// ignore
			} catch (Throwable t) {
				throw new IllegalStateException(t);
			} finally {
				IoHelper.close(br);
				IoHelper.close(is);
			}
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

		String brexOpt = "";
		if (beginRegex != null)
			brexOpt = " brex=" + quote(brexOpt);
		String erexOpt = "";
		if (endRegex != null)
			erexOpt = " erex=" + quote(erexOpt);
		String dfOpt = "";
		if (dateFormat != null)
			dfOpt = " df=" + quote(dateFormat);
		String dpOpt = "";
		if (datePattern != null)
			dpOpt = " dp=" + quote(datePattern);
		String csOpt = "";
		if (charset != null)
			csOpt = " cs=" + charset;
		String fileTagOpt = "";
		if (fileTag != null)
			fileTagOpt = " file_tag=" + fileTag;

		return "textfile" + offsetOpt + limitOpt + brexOpt + erexOpt + dfOpt + dpOpt + csOpt + fileTagOpt + " " + filePath;
	}

	private String quote(String s) {
		return "\"" + s.replaceAll("\"", "\\\"") + "\"";
	}

}
