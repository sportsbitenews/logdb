/**
 * Copyright 2014 Eediom Inc.
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
package org.araqne.logdb.writer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.Row;
import org.araqne.logdb.query.command.IoHelper;
import org.json.JSONConverter;
import org.json.JSONException;

/**
 * @author darkluster
 */
public class JsonLineWriter implements LineWriter {
	private FileOutputStream fos;
	private BufferedOutputStream bos;
	private OutputStreamWriter osw;
	private String lineSeparator;
	private List<String> fields;
	private boolean hasFields;

	public JsonLineWriter(String filePath, List<String> fields, String encoding, boolean append) throws IOException {
		this.fos = new FileOutputStream(new File(filePath), append);
		this.bos = new BufferedOutputStream(fos);
		this.osw = new OutputStreamWriter(bos, Charset.forName(encoding));
		this.lineSeparator = System.getProperty("line.separator");
		this.fields = fields;
		this.hasFields = !fields.isEmpty();
	}

	@Override
	public synchronized void write(Row m) throws IOException {
		String line = null;
		try {
			if (!hasFields) {
				line = JSONConverter.jsonize(m.map());
			} else {
				Map<String, Object> origin = m.map();
				HashMap<String, Object> json = new HashMap<String, Object>();
				for (String field : fields) {
					json.put(field, origin.get(field));
				}

				line = JSONConverter.jsonize(json);
			}

			osw.write(line);
		} catch (JSONException e) {
			throw new IOException(e);
		}

		osw.write(lineSeparator);
	}

	@Override
	public synchronized void flush() throws IOException {
		osw.flush();
	}

	@Override
	public synchronized void close() throws IOException {
		IoHelper.close(osw);
		IoHelper.close(bos);
		IoHelper.close(fos);
	}
}
