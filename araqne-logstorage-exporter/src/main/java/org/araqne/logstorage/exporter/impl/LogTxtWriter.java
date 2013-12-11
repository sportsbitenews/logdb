package org.araqne.logstorage.exporter.impl;

import java.io.BufferedOutputStream;
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

public class LogTxtWriter implements LogWriter {
	private List<String> fields;
	private OutputStream os;
	private BufferedOutputStream bos;
	private OutputStreamWriter osw;
	private String lineSeparator;
	private boolean useStandardOutput;
	private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");

	public LogTxtWriter(File f, ExportOption option) {
		this.fields = option.getColumns();
		this.lineSeparator = System.getProperty("line.separator");

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

			bos = new BufferedOutputStream(os);
			osw = new OutputStreamWriter(bos, Charset.forName("utf-8"));
		} catch (Exception e) {
			throw new IllegalStateException("cannot set output file", e);
		}
	}

	@Override
	public void write(Map<String, Object> log) {
		if (osw == null)
			throw new IllegalStateException("does not set output file");

		StringBuilder sb = new StringBuilder(1024);
		for (int index = 0; index < fields.size(); index++) {
			String field = fields.get(index);

			Object o = log.get(field);
			if (o != null && o instanceof Date) {
				o = df.format(o);
			}
			sb.append(o == null ? "" : o.toString());
			if (index != fields.size() - 1)
				sb.append(",");
		}

		try {
			osw.write(sb.toString());
			osw.write(lineSeparator);
		} catch (IOException e) {
			throw new IllegalStateException("cannot wirte log", e);
		}
	}

	@Override
	public void close() {
		if (osw != null)
			try {
				osw.flush();
			} catch (IOException e1) {
			}
		if (os != null)
			try {
				os.close();
			} catch (IOException e) {
			}
		if (bos != null)
			try {
				bos.close();
			} catch (IOException e) {
			}
		if (osw != null)
			try {
				osw.close();
			} catch (IOException e) {
			}
	}
}
