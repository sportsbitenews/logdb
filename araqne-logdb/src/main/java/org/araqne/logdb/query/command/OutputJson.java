/*
 * Copyright 2013 Eediom Inc.
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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.Strings;
import org.json.JSONConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class OutputJson extends QueryCommand {
	private final Logger logger = LoggerFactory.getLogger(OutputJson.class.getName());
	private FileOutputStream fos;
	private OutputStreamWriter osw;
	private List<String> fields;
	private String lineSeparator;
	private BufferedOutputStream bos;
	private final boolean hasFields;
	private File f;
	private String filePathToken;
	private boolean overwrite;

	public OutputJson(File f, String filePathToken, boolean overwrite, List<String> fields) {
		this.f = f;
		this.overwrite = overwrite;
		this.filePathToken = filePathToken;
		this.fields = fields;
		this.fos = null;
		this.bos = null;
		this.osw = null;
		try {
			this.fos = new FileOutputStream(f);
			this.bos = new BufferedOutputStream(fos);
			this.osw = new OutputStreamWriter(bos, Charset.forName("utf-8"));
			this.lineSeparator = System.getProperty("line.separator");
			this.hasFields = !fields.isEmpty();
		} catch (Throwable t) {
			close();
			throw new QueryParseException("io-error", -1);
		}
	}

	public File getTxtFile() {
		return f;
	}

	public List<String> getFields() {
		return fields;
	}

	@Override
	public void onPush(Row m) {
		try {
			HashMap<String, Object> json = new HashMap<String, Object>();

			Map<String, Object> origin = m.map();
			for (String field : hasFields ? fields : origin.keySet()) {
				json.put(field, origin.get(field));
			}

			osw.write(JSONConverter.jsonize(json));
			osw.write(lineSeparator);
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: cannot write log to json file", t);

			getQuery().stop(QueryStopReason.CommandFailure);
		}
		pushPipe(m);
	}

	@Override
	public boolean isReducer() {
		return true;
	}

	@Override
	public void onClose(QueryStopReason reason) {
		close();
		if (reason == QueryStopReason.CommandFailure)
			f.delete();
	}

	private void close() {
		IoHelper.close(osw);
		IoHelper.close(bos);
		IoHelper.close(fos);
	}

	@Override
	public String toString() {
		String overwriteOption = " ";
		if (overwrite)
			overwriteOption = " overwrite=true ";

		String fieldsOption = "";
		if (!fields.isEmpty())
			fieldsOption = " " + Strings.join(fields, ", ");

		return "outputjson" + overwriteOption + filePathToken + fieldsOption;
	}
}
