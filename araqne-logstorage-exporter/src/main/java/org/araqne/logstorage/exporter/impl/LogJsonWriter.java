package org.araqne.logstorage.exporter.impl;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.araqne.logstorage.exporter.ExportOption;
import org.araqne.logstorage.exporter.api.LogWriter;
import org.json.JSONConverter;

public class LogJsonWriter implements LogWriter {
	private List<String> fields;
	private OutputStream os;
	private BufferedOutputStream bos;
	private OutputStreamWriter osw;
	private String lineSeparator;
	private boolean useStandardOutput;

	public LogJsonWriter(File f, ExportOption option) {
		this.fields = option.getColumns();
		this.lineSeparator = System.getProperty("line.separator");

		try {
			useStandardOutput = option.isUseStandardOutput();
			if (useStandardOutput)
				return;

			if (option.isUseCompress())
				os = new GZIPOutputStream(new FileOutputStream(f));
			else
				os = new FileOutputStream(f);
			bos = new BufferedOutputStream(os);
			osw = new OutputStreamWriter(bos, Charset.forName("utf-8"));

		} catch (Exception e) {
			throw new IllegalStateException("cannot set output file", e);
		}
	}

	@Override
	public void write(Map<String, Object> log) {
		if (!useStandardOutput && osw == null)
			throw new IllegalStateException("does not set output file");

		HashMap<String, Object> json = new HashMap<String, Object>();
		for (String field : fields) {
			json.put(field, log.get(field));
		}

		try {
			String jsonize = JSONConverter.jsonize(json);
			if (useStandardOutput) {
				System.out.println();
				return;
			}
			osw.write(jsonize);
			osw.write(lineSeparator);
		} catch (Exception e) {
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
