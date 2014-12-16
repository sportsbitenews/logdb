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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.araqne.logdb.Row;
import org.araqne.logdb.query.command.IoHelper;

/**
 * @author mindori
 */
public class PlainLineWriter implements LineWriter {
	private static final int MAX_ROW_COUNT = 100;
	private OutputStream fos;
	private List<Row> rowBuffer;
	private Charset charset;
	private List<String> fields;
	private byte[] delimiterBlob;
	private byte[] lineSeparator;
	private ByteArrayOutputStream bos = new ByteArrayOutputStream(10 * 1024);

	public PlainLineWriter(String filePath, List<String> fields, String encoding, boolean append, String delimiter)
			throws IOException {
		fos = new FileOutputStream(new File(filePath), append);
		rowBuffer = new ArrayList<Row>(MAX_ROW_COUNT);
		this.fields = fields;
		setEncoding(encoding);
		lineSeparator = System.getProperty("line.separator").getBytes();
		delimiter = (delimiter == null) ? " " : delimiter;
		delimiterBlob = delimiter.getBytes();
	}

	private void setEncoding(String encoding) {
		try {
			charset = Charset.forName(encoding);
		} catch (Exception e) {
			charset = Charset.forName("utf-8");
		}
	}

	@Override
	public synchronized void write(Row m) throws IOException {
		rowBuffer.add(m);

		if (rowBuffer.size() >= MAX_ROW_COUNT)
			flush();
	}

	@Override
	public synchronized void flush() throws IOException {
		int last = fields.size() - 1;
		int n = 0;

		bos.reset();
		for (Row m : rowBuffer) {
			for (String field : fields) {
				Object o = m.get(field);
				String rowStr = (o == null) ? "" : o.toString();
				byte[] b = rowStr.toString().getBytes(charset);
				bos.write(b);

				if (n++ != last) {
					bos.write(delimiterBlob);
				}
			}
			bos.write(lineSeparator);
		}

		fos.write(bos.toByteArray());
		rowBuffer.clear();
	}

	@Override
	public synchronized void close() throws IOException {
		flush();
		IoHelper.close(fos);
	}
}