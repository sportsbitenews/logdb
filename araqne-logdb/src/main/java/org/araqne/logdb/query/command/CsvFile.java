package org.araqne.logdb.query.command;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.Row;

import au.com.bytecode.opencsv.CSVReader;

public class CsvFile extends DriverQueryCommand {
	private List<String> filePaths;
	private String filePath;
	private long offset;
	private long limit;
	private final String cs;

	public CsvFile(List<String> filePaths, String filePath, long offset, long limit, String cs) {
		this.filePaths = filePaths;
		this.filePath = filePath;
		this.offset = offset;
		this.limit = limit;
		this.cs = cs;
	}

	@Override
	public String getName() {
		return "csvfile";
	}

	@Override
	public void run() {
		for (String filePath : filePaths)
			readCsvFile(filePath);
	}

	private void readCsvFile(String filePath) {
		FileInputStream is = null;
		CSVReader reader = null;

		long p = 0;
		long count = 0;

		try {
			File f = new File(filePath);
			is = new FileInputStream(f);
			reader = new CSVReader(new InputStreamReader(is, cs), ',', '\"', '\0');
			String[] headers = reader.readNext();
			int headerCount = headers.length;
			while (true) {
				String[] items = reader.readNext();
				if (items == null)
					break;

				p++;

				if (p <= offset)
					continue;

				if (limit != 0 && count >= limit)
					break;

				int itemCount = items.length;

				Map<String, Object> m = new HashMap<String, Object>();
				for (int i = 0; i < Math.min(headerCount, itemCount); i++) {
					m.put(headers[i], items[i]);
				}

				if (itemCount > headerCount) {
					for (int i = headerCount; i < itemCount; i++) {
						m.put("column" + i, items[i]);
					}
				}

				m.put("_file", f.getName());

				pushPipe(new Row(m));
				count++;

			}

		} catch (Throwable t) {
			throw new RuntimeException("csvfile load failure", t);
		} finally {
			IoHelper.close(reader);
			IoHelper.close(is);
		}
	}

	@Override
	public String toString() {
		String s = "csvfile";

		if (offset > 0)
			s += " offset=" + offset;

		if (limit > 0)
			s += " limit=" + limit;

		if (!cs.equals("utf-8"))
			s += " cs=" + cs;

		s += " " + filePath;
		return s;
	}

}
