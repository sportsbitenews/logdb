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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.araqne.logdb.Row;
import org.araqne.logdb.query.command.IoHelper;

/**
 * @author darkluster
 */
public class PlainLineWriter implements LineWriter {
	private FileOutputStream fos;
	private BufferedOutputStream bos;
	private OutputStreamWriter osw;
	private List<String> fields;
	private String lineSeparator;
	private String delimiter;

	public PlainLineWriter(String filePath, List<String> fields, String delimiter, String encoding) throws IOException {
		this.fos = new FileOutputStream(new File(filePath));
		this.bos = new BufferedOutputStream(fos);
		this.osw = new OutputStreamWriter(bos, Charset.forName(encoding));
		this.fields = fields;
		this.lineSeparator = System.getProperty("line.separator");
		this.delimiter = delimiter;
	}

	@Override
	public void write(Row m) throws IOException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
		int n = 0;
		int last = fields.size() - 1;
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

	@Override
	public void close() throws IOException {
		IoHelper.close(osw);
		IoHelper.close(bos);
		IoHelper.close(fos);
	}
}
