package org.araqne.logdb.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.LookupHandler2;
import org.araqne.logdb.LookupTable;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

public class CsvLookupHandler implements LookupHandler2 {
	private final Logger slog = LoggerFactory.getLogger(CsvLookupHandler.class);
	private String keyFieldName;
	private ArrayList<String> valueFieldNames;
	private Map<String, Map<String, String>> mappings = new HashMap<String, Map<String, String>>();

	public CsvLookupHandler(File f) throws IOException {
		CSVReader reader = null;
		FileInputStream is = null;

		try {
			int skipBytes = getBomLength(f);

			is = new FileInputStream(f);
			is.skip(skipBytes);

			reader = new CSVReader(new InputStreamReader(is, "utf-8"));

			String[] nextLine = reader.readNext();
			if (nextLine == null)
				throw new IllegalStateException("header columns not found");

			if (nextLine.length < 2)
				throw new IllegalStateException("not enough columns (should be 2 or more)");

			keyFieldName = nextLine[0];
			valueFieldNames = new ArrayList<String>(nextLine.length - 1);
			for (int i = 1; i < nextLine.length; i++)
				valueFieldNames.add(nextLine[i]);

			if (slog.isDebugEnabled())
				slog.debug("araqne logdb: key field [{}] value fields [{}]", keyFieldName, valueFieldNames);

			while ((nextLine = reader.readNext()) != null) {
				Map<String, String> values = new HashMap<String, String>();
				for (int i = 1; i < nextLine.length; i++) {
					String valueFieldName = valueFieldNames.get(i - 1);
					String value = nextLine[i];
					values.put(valueFieldName, value);
				}

				mappings.put(nextLine[0], values);
			}
		} finally {
			if (reader != null)
				reader.close();
		}
	}

	// support utf8 BOM only
	private int getBomLength(File f) throws IOException {
		if (f.length() < 3)
			return 0;

		byte[] buf = new byte[3];
		byte[] bom = new byte[] { (byte) 0xef, (byte) 0xbb, (byte) 0xbf };
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(f, "r");
			raf.readFully(buf);

			if (Arrays.equals(buf, bom))
				return 3;
		} finally {
			if (raf != null) {
				try {
					raf.close();
				} catch (IOException e) {
				}
			}
		}

		return 0;
	}

	@Override
	public Object lookup(String srcField, String dstField, Object srcValue) {
		if (srcValue == null)
			return null;

		if (!valueFieldNames.contains(dstField))
			return null;

		Map<String, String> valueMappings = mappings.get(srcValue.toString());
		if (valueMappings == null)
			return null;

		return valueMappings.get(dstField);
	}

	@Override
	public LookupTable newTable(String keyField, Map<String, String> outputFields) {
		return new CsvLookupTable(keyField, outputFields, mappings);
	}

	private class CsvLookupTable implements LookupTable {
		private String keyFieldName;
		private Map<String, Map<String, String>> mappings = new HashMap<String, Map<String, String>>();
		private Map<String, String> outputFields;

		public CsvLookupTable(String keyFieldName, Map<String, String> outputFields, Map<String, Map<String, String>> mappings) {
			this.keyFieldName = keyFieldName;
			this.mappings = mappings;
			this.outputFields = outputFields;
		}

		@Override
		public void lookup(Row row) {
			csvLookup(row);
		}

		@Override
		public void lookup(RowBatch rowBatch) {
			if (rowBatch.selectedInUse) {
				for (int i = 0; i < rowBatch.size; i++) {
					int p = rowBatch.selected[i];
					Row row = rowBatch.rows[p];
					csvLookup(row);
				}
			} else {
				for (int i = 0; i < rowBatch.size; i++) {
					Row row = rowBatch.rows[i];
					csvLookup(row);
				}
			}
		}

		private void csvLookup(Row row) {
			Object key = row.get(keyFieldName);
			if (key == null)
				return;

			String keyString = key.toString();

			Map<String, String> values = mappings.get(keyString);
			if (values == null || values.isEmpty())
				return;

			for (String outputField : outputFields.keySet()) {
				String renameField = outputFields.get(outputField);

				String value = values.get(outputField);
				if (value != null)
					row.put(renameField, value);
			}
		}
	}
}
