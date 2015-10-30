package org.araqne.log.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvParser {
	private final char qoute = '"';
	private final char delimiter;
	private final char escape;
	private final ArrayList<String> cachedColumnHeaders = new ArrayList<String>();

	public CsvParser(boolean useTab, boolean useDoubleQuote, String[] columnHeaders) {
		this.delimiter = useTab ? '\t' : ',';
		this.escape = useDoubleQuote ? '"' : '\\';

		if (columnHeaders != null)
			for (String header : columnHeaders)
				cachedColumnHeaders.add(header);
	}

	public Map<String, Object> parse(String line) {
		boolean containEscape = false;
		boolean openQuote = false;
		boolean openChar = false;
		boolean checkEnd = false;
		int startIndex = 0;
		int endIndex = 0;
		int length = line.length();

		List<String> values = new ArrayList<String>();
		for (int i = 0; i < length; i++) {
			char c = line.charAt(i);
			if (!openChar && c == qoute) {
				if (!openQuote)
					startIndex = i + 1;
				else {
					endIndex = i;
					checkEnd = true;
				}

				openQuote = !openQuote;
			} else if (openQuote && c == escape) {
				if (!containEscape)
					containEscape = true;

				if (!openQuote && !openChar) {
					openChar = true;
					startIndex = i;
				}

				i++;

				if (openChar)
					endIndex = i + 1;

			} else if (c == delimiter && !openQuote) {
				String value = line.substring(startIndex, endIndex);
				if (containEscape) {
					value = removeEscape(value, escape);
					containEscape = false;
				}
				values.add(value);
				startIndex = i + 1;
				endIndex = i + 1;
				openChar = false;
				checkEnd = false;
			} else {
				if (!openQuote && !openChar) {
					startIndex = i;
					openChar = true;
				}

				if ((!checkEnd && openChar) || openQuote)
					endIndex = i + 1;
			}
		}

		if (endIndex > length)
			throw new IllegalArgumentException("invalid csv parse option. delimiter [" + delimiter + "], escape [" + escape
					+ "], line [" + line + "]");

		String value = line.substring(startIndex, endIndex);
		if (containEscape)
			value = removeEscape(value, escape);
		values.add(value);

		return toMap(values);
	}

	private Map<String, Object> toMap(List<String> values) {
		Map<String, Object> m = new HashMap<String, Object>(values.size());
		int i = 0;
		for (String value : values) {
			String header = null;
			try {
				header = cachedColumnHeaders.get(i);
			} catch (IndexOutOfBoundsException e) {
				header = "column" + i;
				cachedColumnHeaders.add(header);
			}

			m.put(header, value);
			i++;
		}
		return m;
	}

	private String removeEscape(String value, char escape) {
		StringBuilder sb = new StringBuilder(value.length());
		for (int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);
			if (c == escape) {
				sb.append(value.charAt(++i));
				continue;
			}
			sb.append(c);
		}
		return sb.toString();
	}
}