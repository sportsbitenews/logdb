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

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.Strings;

import au.com.bytecode.opencsv.CSVWriter;

public class OutputCsv extends QueryCommand {
	private Charset charset;
	private List<String> fields;

	// for query string generation
	private String pathToken;
	private File f;
	private boolean overwrite;
	private FileOutputStream os;
	private CSVWriter writer;
	private String[] csvLine;

	public OutputCsv(String pathToken, File f, boolean overwrite, List<String> fields, String encoding, boolean useBom, boolean useTab)
			throws IOException {
		this.pathToken = pathToken;
		this.f = f;
		this.overwrite = overwrite;
		this.os = new FileOutputStream(f);
		if (useBom)
			writeBom(encoding, os);
		this.fields = fields;
		this.charset = Charset.forName(encoding);
		this.csvLine = new String[fields.size()];
		
		char separator = useTab ? '\t' : ','; 
		this.writer = new CSVWriter(new OutputStreamWriter(os, charset), separator);
		writer.writeNext(fields.toArray(new String[0]));
	}

	private void writeBom(String encoding, FileOutputStream fos) throws IOException {
		if (encoding.equalsIgnoreCase("utf-8")) {
			fos.write(0xEF);
			fos.write(0xBB);
			fos.write(0xBF);
		} else if (encoding.equalsIgnoreCase("utf-16") || encoding.equalsIgnoreCase("utf-16le")) {
			fos.write(0xFF);
			fos.write(0xFE);
		} else if (encoding.equalsIgnoreCase("utf-16be")) {
			fos.write(0xFE);
			fos.write(0xFF);
		} else if (encoding.equalsIgnoreCase("utf-32") || encoding.equalsIgnoreCase("utf-32le")) {
			fos.write(0xFF);
			fos.write(0xFE);
			fos.write(0x00);
			fos.write(0x00);
		} else if (encoding.equalsIgnoreCase("utf-32be")) {
			fos.write(0x00);
			fos.write(0x00);
			fos.write(0xFE);
			fos.write(0xFF);
		} else if (encoding.equalsIgnoreCase("utf-1")) {
			fos.write(0xF7);
			fos.write(0x64);
			fos.write(0x4C);
		} else if (encoding.equalsIgnoreCase("utf-ebcdic")) {
			fos.write(0xDD);
			fos.write(0x73);
			fos.write(0x66);
			fos.write(0x73);
		} else if (encoding.equalsIgnoreCase("scsu")) {
			fos.write(0x02);
			fos.write(0xFE);
			fos.write(0xFF);
		} else if (encoding.equalsIgnoreCase("bocu-1")) {
			fos.write(0xFB);
			fos.write(0xEE);
			fos.write(0x28);
		} else if (encoding.equalsIgnoreCase("gb-18030")) {
			fos.write(0x84);
			fos.write(0x31);
			fos.write(0x95);
			fos.write(0x33);
		}
	}

	@Override
	public String getName() {
		return "outputcsv";
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
		if (reason == QueryStopReason.CommandFailure)
			f.delete();
	}

	@Override
	public String toString() {
		String overwriteOption = " ";
		if (overwrite)
			overwriteOption = " overwrite=true ";

		return "outputcsv" + overwriteOption + pathToken + " " + Strings.join(fields, ", ");
	}
}
