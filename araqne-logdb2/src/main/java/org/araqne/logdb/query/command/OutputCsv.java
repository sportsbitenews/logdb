/*
 * Copyright 2012 Future Systems
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.List;

import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.impl.Strings;

import au.com.bytecode.opencsv.CSVWriter;

public class OutputCsv extends QueryCommand {
	private Charset utf8;
	private List<String> fields;

	// for query string generation
	private String pathToken;
	private File f;
	private boolean overwrite;
	private FileOutputStream os;
	private CSVWriter writer;
	private String[] csvLine;

	public OutputCsv(String pathToken, File f, boolean overwrite, List<String> fields) throws IOException {
		this.pathToken = pathToken;
		this.f = f;
		this.overwrite = overwrite;
		this.os = new FileOutputStream(f);
		this.fields = fields;
		this.utf8 = Charset.forName("utf-8");
		this.csvLine = new String[fields.size()];
		this.writer = new CSVWriter(new OutputStreamWriter(os, utf8));
		writer.writeNext(fields.toArray(new String[0]));
	}

	public File getCsvFile() {
		return f;
	}

	public boolean isOverwrite() {
		return overwrite;
	}

	public List<String> getFields() {
		return fields;
	}

	@Override
	public void onPush(Row m) {
		int i = 0;
		for (String field : fields) {
			Object o = m.get(field);
			String s = o == null ? "" : o.toString();
			csvLine[i++] = s;
		}

		writer.writeNext(csvLine);
		pushPipe(m);
	}

	@Override
	public boolean isReducer() {
		return true;
	}

	@Override
	public void onClose(QueryStopReason reason) {
		this.status = Status.Finalizing;
		try {
			writer.flush();
		} catch (IOException e1) {
		}

		IoHelper.close(os);
	}

	@Override
	public String toString() {
		String overwriteOption = " ";
		if (overwrite)
			overwriteOption = " overwrite=true ";

		return "outputcsv" + overwriteOption + pathToken + " " + Strings.join(fields, ", ");
	}
}