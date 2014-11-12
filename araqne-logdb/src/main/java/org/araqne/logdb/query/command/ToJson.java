/*
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
package org.araqne.logdb.query.command;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.json.JSONConverter;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ToJson extends QueryCommand {
	private final Logger logger = LoggerFactory.getLogger(ToJson.class.getName());
	private List<String> fields;
	private final String output;
	private final boolean hasFields;

	public ToJson(List<String> fields, String output) {
		this.fields = fields;
		this.output = output;
		this.hasFields = !fields.isEmpty();
	}
	
	@Override
	public String getName() {
		return "tojson";
	}
	
	private Row jsonify(Row m) throws JSONException {
		String line = null;
		if (!hasFields) {
			line = JSONConverter.jsonize(m.map());
		} else {
			Map<String, Object> origin = m.map();
			HashMap<String, Object> json = new HashMap<String, Object>();
			for (String field : fields) {
				json.put(field, origin.get(field));
			}

			line = JSONConverter.jsonize(json);
		}
		
		m.put(output, line);
		return m;
	}
	
	@Override
	public void onPush(Row m) {
		try {
			jsonify(m);
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: cannot convert to json", t);
			
			getQuery().stop(QueryStopReason.CommandFailure);
		}
		
		pushPipe(m);
	}
	
	@Override
	public void onPush(RowBatch rowBatch) {
		try {
			if (rowBatch.selectedInUse) {
				for (int i = 0; i < rowBatch.size; i++) {
					int p = rowBatch.selected[i];
					Row m = rowBatch.rows[p];

					jsonify(m);
				}
			} else {
				for (Row m : rowBatch.rows) {
					jsonify(m);
				}
			}
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: cannot convert to json", t);

			getQuery().stop(QueryStopReason.CommandFailure);
		}

		pushPipe(rowBatch);
	}

}
