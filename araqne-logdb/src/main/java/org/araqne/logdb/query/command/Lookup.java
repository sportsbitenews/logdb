/*
 * Copyright 2011 Future Systems
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

import org.araqne.logdb.LookupHandler;
import org.araqne.logdb.LookupHandlerRegistry;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;

public class Lookup extends QueryCommand {
	private LookupHandlerRegistry registry;
	private String handlerName;
	private String lookupInputField;
	private String sourceField;
	private String lookupOutputField;
	private String targetField;

	public Lookup(String handlerName, String srcField, String dstField) {
		this(handlerName, srcField, srcField, dstField, dstField);
	}

	public Lookup(String handlerName, String sourceField, String lookupInputField, String lookupOutputField, String targetField) {
		this.handlerName = handlerName;
		this.sourceField = sourceField;
		this.lookupInputField = lookupInputField;
		this.lookupOutputField = lookupOutputField;
		this.targetField = targetField;
	}

	public String getHandlerName() {
		return handlerName;
	}

	public String getSourceField() {
		return sourceField;
	}

	public String getLookupInputField() {
		return lookupInputField;
	}

	public String getLookupOutputField() {
		return lookupOutputField;
	}

	public String getTargetField() {
		return targetField;
	}

	public void setLogQueryService(LookupHandlerRegistry registry) {
		this.registry = registry;
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		LookupHandler handler = registry.getLookupHandler(handlerName);

		if (handler == null) {
			// bypass without lookup
			pushPipe(rowBatch);
			return;
		}

		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[rowBatch.selected[i]];
				Object value = row.get(sourceField);
				row.put(targetField, handler.lookup(lookupInputField, lookupOutputField, value));
			}
		} else {
			for (Row row : rowBatch.rows) {
				Object value = row.get(sourceField);
				row.put(targetField, handler.lookup(lookupInputField, lookupOutputField, value));
			}
		}

		pushPipe(rowBatch);
	}

	@Override
	public void onPush(Row m) {
		Object value = m.get(sourceField);
		LookupHandler handler = registry.getLookupHandler(handlerName);
		if (handler != null)
			m.put(targetField, handler.lookup(lookupInputField, lookupOutputField, value));
		pushPipe(m);
	}

	@Override
	public boolean isReducer() {
		return false;
	}

	@Override
	public String toString() {
		String input = sourceField;
		if (!lookupInputField.equals(sourceField))
			input += " as " + lookupInputField;

		String output = lookupOutputField;
		if (!lookupOutputField.equals(targetField))
			output += " as " + targetField;

		return "lookup " + handlerName + " " + input + " output " + output;
	}
}
