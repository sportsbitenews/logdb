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
package org.araqne.logdb.cep.query;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.query.expr.Expression;

public class CepDelCommand extends QueryCommand {
	private EventContextStorage storage;
	private String topic;
	private String keyField;
	private Expression matcher;

	public CepDelCommand(EventContextStorage storage, String topic, String keyField, Expression matcher) {
		this.storage = storage;
		this.topic = topic;
		this.keyField = keyField;
		this.matcher = matcher;
	}

	@Override
	public String getName() {
		return "cepdel";
	}

	@Override
	public void onPush(Row row) {
		boolean matched = true;

		Object o = matcher.eval(row);
		if (o == null)
			matched = false;

		if (o instanceof Boolean && !(Boolean) o)
			matched = false;

		Object k = row.get(keyField);
		if (k == null)
			matched = false;

		if (matched) {
			String key = k.toString();
			EventKey eventKey = new EventKey(topic, key);

			EventContext ctx = storage.getContext(eventKey);
			if (ctx != null)
				ctx.addRow(row);

			storage.removeContext(eventKey);
		}

		pushPipe(row);
	}

}
