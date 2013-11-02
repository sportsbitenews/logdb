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
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class OutputTxt extends LogQueryCommand {
	private final Logger logger = LoggerFactory.getLogger(OutputTxt.class.getName());
	private FileOutputStream fos;
	private List<String> fields;
	private BufferedOutputStream bos;
	private OutputStreamWriter osw;
	private SimpleDateFormat sdf;
	private String lineSeparator;
	private String delimiter;
	private File f;

	public OutputTxt(File f, String delimiter, List<String> fields) throws IOException {
		try {
			this.f = f;
			this.fields = fields;
			this.fos = new FileOutputStream(f);
			this.bos = new BufferedOutputStream(fos);
			this.osw = new OutputStreamWriter(bos, Charset.forName("utf-8"));
			this.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
			this.lineSeparator = System.getProperty("line.separator");
			this.delimiter = delimiter;
		} catch (Throwable t) {
			close();
			throw new LogQueryParseException("io-error", -1);
		}
	}

	public File getTxtFile() {
		return f;
	}

	public List<String> getFields() {
		return fields;
	}

	public String getDelimiter() {
		return delimiter;
	}

	@Override
	public void push(LogMap m) {
		try {
			for (int index = 0; index < fields.size(); index++) {
				String field = fields.get(index);
				Object o = m.get(field);
				String s = o == null ? "" : o.toString();
				if (o instanceof Date)
					s = sdf.format(o);
				osw.write(s);
				if (index != fields.size() - 1)
					osw.write(delimiter);
			}
			osw.write(lineSeparator);
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: cannot write log to txt file", t);
			eof(true);
		}
		write(m);
	}

	@Override
	public boolean isReducer() {
		return true;
	}

	@Override
	public void eof(boolean cancelled) {
		close();
		super.eof(cancelled);
	}

	private void close() {
		try {
			if (this.osw != null)
				osw.close();
		} catch (IOException e) {
		}

		try {
			if (bos != null)
				bos.close();
		} catch (IOException e) {
		}

		try {
			if (fos != null)
				fos.close();
		} catch (IOException e) {
		}
	}
}
