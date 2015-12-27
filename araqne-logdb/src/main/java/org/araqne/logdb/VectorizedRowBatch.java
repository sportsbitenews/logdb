package org.araqne.logdb;

import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.expr.VectorizedExpression;

public class VectorizedRowBatch {
	public boolean selectedInUse;
	public int[] selected;
	public int size;
	public Map<Object, FieldValues> fieldValues;

	public FieldValues eval(Expression expr) {
		if (expr instanceof VectorizedExpression) {
			return ((VectorizedExpression) expr).evalVector(this);
		}

		FieldValues values = new FieldValues(size);
		Object[] objs = values.objs;
		int[] types = values.types;
		for (int i = 0; i < size; i++) {
			Object o = expr.eval(getRow(i));
			if (o != null) {
				objs[i] = o;
				types[i] = getType(o);
			}
		}
		return values;
	}

	private int getType(Object o) {
		if (o instanceof Integer || o instanceof Long)
			return 1;
		else
			return 3;
	}

	public Row getRow(int i) {
		Row row = new Row();
		for (Object key : fieldValues.keySet()) {
			FieldValues values = fieldValues.get(key);
			Object value = null;
			if (values.types[i] == 1)
				value = values.longs[i];
			else if (values.types[i] == 2)
				value = values.doubles[i];
			else
				value = values.objs[i];

			putRecursively(row.map(), key, 0, value);
		}

		return row;
	}

	public RowBatch toRowBatch() {
		RowBatch rowBatch = new RowBatch();
		rowBatch.size = size;
		Row[] rows = new Row[size];
		for (int i = 0; i < size; i++) {
			rows[i] = new Row();
		}
		rowBatch.rows = rows;

		for (Object key : fieldValues.keySet()) {
			FieldValues values = fieldValues.get(key);

			if (selectedInUse) {
				for (int i = 0; i < size; i++) {
					int p = selected[i];
					int type = values.types[p];
					Object value = null;
					switch (type) {
					case 1:
						value = values.longs[p];
						break;
					case 3:
						value = values.objs[p];
						break;
					}

					putRecursively(rows[i].map(), key, 0, value);
				}
			} else {
				for (int i = 0; i < size; i++) {
					int type = values.types[i];
					Object value = null;
					switch (type) {
					case 1:
						value = values.longs[i];
						break;
					case 3:
						value = values.objs[i];
						break;
					}

					putRecursively(rows[i].map(), key, 0, value);
				}
			}
		}

		return rowBatch;
	}

	@SuppressWarnings("unchecked")
	private void putRecursively(Map<String, Object> m, Object key, int depth, Object value) {
		if (key instanceof String) {
			m.put(key.toString(), value);
		} else {
			String[] keys = (String[]) key;
			if (keys.length - 1 == depth) {
				m.put(keys[depth], value);
			} else {
				Map<String, Object> nestedMap = (Map<String, Object>) m.get(keys[depth]);
				if (nestedMap == null) {
					nestedMap = new HashMap<String, Object>();
					m.put(keys[depth], nestedMap);
				}

				putRecursively(nestedMap, keys, depth + 1, value);
			}
		}
	}
}
