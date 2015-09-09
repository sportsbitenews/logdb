/**
 * Copyright 2015 Eediom Inc.
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
package org.araqne.logdb.xlsx;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.araqne.api.Io;
import org.araqne.log.api.Log;
import org.araqne.log.api.LogPipe;
import org.araqne.log.api.Logger;
import org.araqne.log.api.SimpleLog;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

public class XlsxExtractor {
	private File file;
	private String sheetName;
	private Logger logger;
	private LogPipe pipe;
	private long offset;
	private long limit;
	private long skip;
	private volatile boolean cancelled;

	private SheetHandler handler;

	public static List<String> getSheetNames(File f) throws IOException, InvalidFormatException {
		List<String> sheetNames = new ArrayList<String>();
		XSSFWorkbook wb = null;
		try {
			wb = new XSSFWorkbook(f);

			for (int i = 0; i < wb.getNumberOfSheets(); i++) {
				XSSFSheet sheet = wb.getSheetAt(i);
				sheetNames.add(sheet.getSheetName());
			}

			return sheetNames;
		} finally {
			if (wb != null)
				wb.close();
		}
	}

	public XlsxExtractor(File file, String sheetName, Logger logger, LogPipe pipe, long offset, long limit, long skip) {
		this.file = file;
		this.sheetName = sheetName;
		this.logger = logger;
		this.pipe = pipe;
		this.offset = offset;
		this.limit = limit;
		this.skip = skip;
	}

	public long getTotalCount() {
		return handler != null ? handler.totalCount : 0;
	}

	public long getOutputCount() {
		return handler != null ? handler.outputCount : 0;
	}

	public void cancel() {
		cancelled = true;
	}

	public void run() throws IOException, SAXException, OpenXML4JException {
		OPCPackage pkg = null;
		XSSFWorkbook wb = null;
		InputStream is = null;
		try {
			pkg = OPCPackage.open(file);
			wb = new XSSFWorkbook(file);
			XSSFReader r = new XSSFReader(pkg);
			SharedStringsTable sst = r.getSharedStringsTable();

			XSSFSheet sheet = wb.getSheet(sheetName);
			String rId = wb.getRelationId(sheet);

			is = r.getSheet(rId);
			InputSource sheetSource = new InputSource(is);
			XMLReader parser = XMLReaderFactory.createXMLReader("com.sun.org.apache.xerces.internal.parsers.SAXParser");
			handler = new SheetHandler(sst);
			parser.setContentHandler(handler);
			parser.parse(sheetSource);

			// last line (consider header line for offset)
			if (++handler.lineIndex > skip + offset + 1) {
				Log log = new SimpleLog(new Date(), logger == null ? null : logger.getFullName(), handler.data);
				pipe.onLog(logger, log);
				handler.outputCount++;
			}

			handler.totalCount++;
		} catch (SAXException e) {
			if (e.getMessage() == null || !e.getMessage().contains("max limit"))
				throw e;

		} finally {
			Io.ensureClose(is);
			Io.ensureClose(pkg);
			Io.ensureClose(wb);
		}
	}

	private class SheetHandler extends DefaultHandler {
		private SharedStringsTable sst;
		private String value;
		private boolean nextIsString;

		private List<String> headers = new ArrayList<String>();

		// current line number
		private long lineIndex;

		// current column number
		private int columnIndex;

		private long totalCount;
		private long outputCount;

		private Map<String, Object> data = new HashMap<String, Object>();

		private SheetHandler(SharedStringsTable sst) {
			this.sst = sst;
		}

		public void startElement(String uri, String localName, String name, Attributes attributes) throws SAXException {
			if (cancelled)
				throw new SAXException("cancelled");

			// c => cell
			if (name.equals("c")) {
				// Print the cell reference
				String cellId = attributes.getValue("r");
				int lineNum = getLineNum(cellId) - 1;

				if (lineIndex != lineNum) {
					columnIndex = 0;

					if (lineIndex >= skip)
						totalCount++;
				}

				if (lineIndex > skip + offset && lineIndex != lineNum) {
					Log log = new SimpleLog(new Date(), logger == null ? null : logger.getFullName(), data);
					pipe.onLog(logger, log);
					outputCount++;

					data = new HashMap<String, Object>();

					if (limit <= outputCount)
						throw new SAXException("max limit reached");
				}

				lineIndex = lineNum;

				// Figure out if the value is an index in the SST
				String cellType = attributes.getValue("t");
				nextIsString = (cellType != null && cellType.equals("s"));
			}

			// Clear contents cache
			value = null;
		}

		private int getLineNum(String cellId) {
			int len = cellId.length();

			int lineNum = 0;

			// skip alphabet
			int i = 0;
			for (i = 0; i < len; i++) {
				char c = cellId.charAt(i);
				if (c >= '0' && c <= '9')
					break;
			}

			for (; i < len; i++) {
				lineNum *= 10;

				char c = cellId.charAt(i);
				if (c >= '0' && c <= '9')
					lineNum += c - '0';
			}
			return lineNum;
		}

		public void endElement(String uri, String localName, String name) throws SAXException {
			// Process the last contents as required.
			// Do now, as characters() may be called more than once
			if (nextIsString) {
				int idx = Integer.parseInt(value);
				value = new XSSFRichTextString(sst.getEntryAt(idx)).toString();
				nextIsString = false;
			}

			// v => contents of a cell
			// Output after we've seen the string contents
			if (name.equals("v")) {
				if (lineIndex == skip) {
					if (value == null || value.trim().isEmpty())
						value = "column" + columnIndex;
					headers.add(value);
				} else {
					if (columnIndex < headers.size()) {
						String key = headers.get(columnIndex);
						data.put(key, value);
					} else {
						data.put("column" + columnIndex, value);
					}
				}

				columnIndex++;
			}
		}

		public void characters(char[] ch, int start, int length) throws SAXException {
			if (value == null)
				value = new String(ch, start, length);
			else
				value += new String(ch, start, length);
		}
	}

}
