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
import java.util.List;

/**
 * @author darkluster
 */
public class GzipLineWriterFactory implements LineWriterFactory {
	private List<String> fields;
	private String delimiter;
	private String encoding;
	private boolean append;

	public GzipLineWriterFactory(List<String> fields, String delimiter, String encoding, boolean append) {
		this.fields = fields;
		this.delimiter = delimiter;
		this.encoding = encoding;
	}

	@Override
	public LineWriter newWriter(String filePath) throws IOException {
		return new GzipLineWriter(filePath, fields, delimiter, encoding, append);
	}
}
