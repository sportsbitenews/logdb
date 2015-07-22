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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author darkluster
 */
public class CsvLineWriterFactory implements LineWriterFactory {
	private List<String> fields;
	private String encoding;
	private char separator;
	private boolean useBom;
	private Map<String, List<Integer>> boms;
	private boolean append;

	public CsvLineWriterFactory(List<String> fields, String encoding, char separator, boolean useBom, boolean append) {
		this.fields = fields;
		this.encoding = encoding;
		this.separator = separator;
		this.useBom = useBom;
		this.boms = getBoms();
		this.append = append;
	}

	@Override
	public LineWriter newWriter(String filePath) throws IOException {
		return new CsvLineWriter(filePath, fields, encoding, separator, useBom, boms, append);
	}

	// TODO Integer -> byte[]
	private Map<String, List<Integer>> getBoms() {
		Map<String, List<Integer>> boms = new HashMap<String, List<Integer>>();
		boms.put("utf-8", Arrays.asList(0xEF, 0xBB, 0xBF));
		boms.put("utf-16", Arrays.asList(0xFF, 0xFE));
		boms.put("utf-16be", Arrays.asList(0xFE, 0xFF));
		boms.put("utf-32", Arrays.asList(0xFF, 0xFE, 0x00, 0x00));
		boms.put("utf-32be", Arrays.asList(0x00, 0x00, 0xFE, 0xFF));
		boms.put("utf-1", Arrays.asList(0xF7, 0x64, 0x4C));
		boms.put("utf-ebcdic", Arrays.asList(0xDD, 0x73, 0x66, 0x73));
		boms.put("scsu", Arrays.asList(0x02, 0xFE, 0xFF));
		boms.put("bocu-1", Arrays.asList(0xFB, 0xEE, 0x28));
		boms.put("gb-18030", Arrays.asList(0x84, 0x31, 0x95, 0x33));
		return boms;
	}
}
