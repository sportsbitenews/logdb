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
package org.araqne.logdb.query.command;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class OutputTxt extends QueryCommand {
	private final Logger logger = LoggerFactory.getLogger(OutputTxt.class.getName());
	private OutputStream os;
	private String[] fields;
	private BufferedOutputStream bos;
	private OutputStreamWriter osw;
	private SimpleDateFormat sdf;
	private String lineSeparator;
	private String delimiter;
	private File f;
	private String filePath;
	private boolean overwrite;

	public OutputTxt(File f, String filePath, boolean overwrite, String delimiter, List<String> fields, boolean useCompression)
			throws IOException {
		try {
			this.f = f;
			this.filePath = filePath;
			this.overwrite = overwrite;
			this.fields = fields.toArray(new String[0]);
			if (useCompression) {
				this.os = new GZIPOutputStream(new FileOutputStream(f), 8192);
				this.osw = new OutputStreamWriter(os, Charset.forName("utf-8"));
			} else {
				this.os = new FileOutputStream(f);
				this.bos = new BufferedOutputStream(os);
				this.osw = new OutputStreamWriter(bos, Charset.forName("utf-8"));
			}
			this.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
			this.lineSeparator = System.getProperty("line.separator");
			this.delimiter = delimiter;
		} catch (Throwable t) {
			close();
			throw new QueryParseException("io-error", -1);
		}
	}

	public File getTxtFile() {
		return f;
	}

	public List<String> getFields() {
		return Arrays.asList(fields);
	}

	public String getDelimiter() {
		return delimiter;
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		try {
			if (rowBatch.selectedInUse) {
				for (int i = 0; i < rowBatch.size; i++) {
					int p = rowBatch.selected[i];
					Row m = rowBatch.rows[p];

					int n = 0;
					int last = fields.length - 1;
					for (String field : fields) {
						Object o = m.get(field);
						String s = o == null ? "" : o.toString();
						if (o instanceof Date)
							s = sdf.format(o);
						osw.write(s);
						if (n++ != last)
							osw.write(delimiter);
					}
					osw.write(lineSeparator);
				}
			} else {
				for (Row m : rowBatch.rows) {
					int n = 0;
					int last = fields.length - 1;
					for (String field : fields) {
						Object o = m.get(field);
						String s = o == null ? "" : o.toString();
						if (o instanceof Date)
							s = sdf.format(o);
						osw.write(s);
						if (n++ != last)
							osw.write(delimiter);
					}
					osw.write(lineSeparator);
				}
			}
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: cannot write log to txt file", t);

			getQuery().stop(QueryStopReason.CommandFailure);
		}

		pushPipe(rowBatch);
	}

	@Override
	public void onPush(Row m) {
		try {
			int n = 0;
			int last = fields.length - 1;
			for (String field : fields) {
				Object o = m.get(field);
				String s = o == null ? "" : o.toString();
				if (o instanceof Date)
					s = sdf.format(o);
				osw.write(s);
				if (n++ != last)
					osw.write(delimiter);
			}
			osw.write(lineSeparator);
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: cannot write log to txt file", t);

			getQuery().stop(QueryStopReason.CommandFailure);
		}
		pushPipe(m);
	}

	@Override
	public boolean isReducer() {
		return true;
	}

	@Override
	public void onClose(QueryStopReason reason) {
		close();
		if (reason == QueryStopReason.CommandFailure)
			f.delete();
	}

	private void close() {
		IoHelper.close(osw);
		IoHelper.close(bos);
		IoHelper.close(os);
	}

	@Override
	public String toString() {
		String overwriteOption = "";
		if (overwrite)
			overwriteOption = " overwrite=true ";

		String delimiterOption = "";
		if (!delimiter.equals(" "))
			delimiterOption = " delimiter=" + delimiter;

		String path = " " + filePath;

		String fieldsOption = "";
		if (fields.length > 0)
			fieldsOption = " " + Strings.join(getFields(), ", ");

		return "outputtxt" + overwriteOption + delimiterOption + path + fieldsOption;
	}
}
