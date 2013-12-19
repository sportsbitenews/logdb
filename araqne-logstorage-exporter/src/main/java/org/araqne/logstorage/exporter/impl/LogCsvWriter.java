package org.araqne.logstorage.exporter.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.araqne.logstorage.exporter.ExportOption;
import org.araqne.logstorage.exporter.api.LogWriter;

import au.com.bytecode.opencsv.CSVWriter;

public class LogCsvWriter implements LogWriter {
	private OutputStream os;
	private CSVWriter writer;
	private List<String> fields;
	private String[] csvLine;
	private boolean useStandardOutput;
	private OutputStreamWriter osw;
	private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");

	public LogCsvWriter(File f, ExportOption option) {
		this.fields = option.getColumns();
		csvLine = new String[fields.size()];

		try {
			useStandardOutput = option.isUseStandardOutput();
			if (useStandardOutput) {
				os = new StandardOutputStream(System.out);
			} else {
				if (option.isUseCompress())
					os = new GZIPOutputStream(new FileOutputStream(f));
				else
					os = new FileOutputStream(f);
			}

			osw = new OutputStreamWriter(os, Charset.forName("utf-8"));
			this.writer = new CSVWriter(osw);
			writer.writeNext(fields.toArray(new String[0]));
		} catch (Exception e) {
			throw new IllegalStateException("cannot set output file", e);
		}
	}

	@Override
	public void write(Map<String, Object> m) {
		if (writer == null)
			throw new IllegalStateException("does not set output file");

		int i = 0;
		for (String field : fields) {
			Object o = m.get(field);
			if (o != null && o instanceof Date) {
				o = df.format(o);
			}
			String s = o == null ? "" : o.toString();
			csvLine[i++] = s;
		}
		writer.writeNext(csvLine);
	}

	@Override
	public void close() {
		if (writer != null)
			try {
				writer.flush();
			} catch (IOException e1) {
			}
		if (os != null)
			try {
				os.close();
			} catch (IOException e) {
			}
		if (writer != null)
			try {
				writer.close();
			} catch (IOException e) {
			}
	}
}
