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
import org.json.JSONException;
import org.json.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class OutputJson extends LogQueryCommand {
	private final Logger logger = LoggerFactory.getLogger(OutputJson.class.getName());
	private FileOutputStream fos;
	private OutputStreamWriter osw;
	private List<String> fields;
	private JSONWriter jwriter;
	private SimpleDateFormat sdf;
	private String lineSeparator;
	private BufferedOutputStream bos;
	private final boolean hasFields;
	private File f;

	public OutputJson(File f, List<String> fields) {
		this.f = f;
		this.fields = fields;
		this.fos = null;
		this.bos = null;
		this.osw = null;
		try {
			this.fos = new FileOutputStream(f);
			this.bos = new BufferedOutputStream(fos);
			this.osw = new OutputStreamWriter(bos, Charset.forName("utf-8"));
			this.jwriter = new JSONWriter(osw);
			this.jwriter.array();
			this.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
			this.lineSeparator = System.getProperty("line.separator");
			this.hasFields = !fields.isEmpty();
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

	@Override
	public void push(LogMap m) {
		try {
			jwriter.object();

			for (String field : hasFields ? fields : m.map().keySet()) {
				Object o = m.get(field);
				String s = o == null ? "" : o.toString();
				if (o instanceof Date) {
					s = sdf.format(o);
				}
				jwriter.key(field).value(s);
			}

			jwriter.endObject();

			osw.write(lineSeparator);
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: cannot write log to json file", t);
			eof(true);
		}
		write(m);
	}

	@Override
	public boolean isReducer() {
		return false;
	}

	@Override
	public void eof(boolean cancelled) {
		try {
			jwriter.endArray();
		} catch (JSONException e1) {
		}
		this.status = Status.Finalizing;

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
