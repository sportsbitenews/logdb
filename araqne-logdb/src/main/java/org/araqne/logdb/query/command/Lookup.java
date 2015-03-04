package org.araqne.logdb.query.command;

import java.util.Map;

import org.araqne.logdb.LookupHandler;
import org.araqne.logdb.LookupHandler2;
import org.araqne.logdb.LookupHandlerRegistry;
import org.araqne.logdb.LookupTable;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;

public class Lookup extends QueryCommand implements ThreadSafe {
	private LookupHandlerRegistry registry;
	private final String handlerName;
	private final String lookupInputField;
	private final String sourceField;

	// original output field to user defined field
	private final Map<String, String> outputFields;
	private LookupHandler handler;
	private LookupTable table;

	public Lookup(LookupHandlerRegistry registry, String handlerName, String sourceField, String lookupInputField,
			Map<String, String> outputFields) {
		this.registry = registry;
		this.handlerName = handlerName;
		this.sourceField = sourceField;
		this.lookupInputField = lookupInputField;
		this.outputFields = outputFields;
	}

	@Override
	public String getName() {
		return "lookup";
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

	public Map<String, String> getOutputFields() {
		return outputFields;
	}

	public void setLookupHandlerRegistry(LookupHandlerRegistry registry) {
		this.registry = registry;
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		if (handler == null)
			handler = registry.getLookupHandler(handlerName);

		if (handler == null) {
			// bypass without lookup
			pushPipe(rowBatch);
			return;
		}

		if (table == null && handler instanceof LookupHandler2)
			table = ((LookupHandler2) handler).newTable(sourceField, outputFields);

		if (table != null) {
			table.lookup(rowBatch);
			pushPipe(rowBatch);
			return;
		}

		// support old lookup handler
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[rowBatch.selected[i]];
				Object lookupKey = row.get(sourceField);

				for (String lookupOutputField : outputFields.keySet()) {
					String rowOutputField = outputFields.get(lookupOutputField);
					row.put(rowOutputField, handler.lookup(lookupInputField, lookupOutputField, lookupKey));
				}
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];

				Object lookupKey = row.get(sourceField);
				for (String lookupOutputField : outputFields.keySet()) {
					String rowOutputField = outputFields.get(lookupOutputField);
					row.put(rowOutputField, handler.lookup(lookupInputField, lookupOutputField, lookupKey));
				}
			}
		}

		pushPipe(rowBatch);
	}

	@Override
	public void onPush(Row row) {
		Object lookupKey = row.get(sourceField);

		if (handler == null)
			handler = registry.getLookupHandler(handlerName);

		if (handler == null) {
			// bypass without lookup
			pushPipe(row);
			return;
		}

		if (table == null && handler instanceof LookupHandler2)
			table = ((LookupHandler2) handler).newTable(sourceField, outputFields);

		if (table != null) {
			table.lookup(row);
			pushPipe(row);
			return;
		}

		for (String lookupOutputField : outputFields.keySet()) {
			String rowOutputField = outputFields.get(lookupOutputField);
			row.put(rowOutputField, handler.lookup(lookupInputField, lookupOutputField, lookupKey));
		}

		pushPipe(row);
	}

	@Override
	public String toString() {
		String input = sourceField;
		if (!lookupInputField.equals(sourceField))
			input += " as " + lookupInputField;

		int i = 0;
		String output = "";
		for (String lookupOutputField : outputFields.keySet()) {
			if (i++ != 0)
				output += ", ";

			output += lookupOutputField;
			String targetField = outputFields.get(lookupOutputField);

			if (!lookupOutputField.equals(targetField))
				output += " as " + targetField;
		}

		return "lookup " + handlerName + " " + input + " output " + output;
	}

}
