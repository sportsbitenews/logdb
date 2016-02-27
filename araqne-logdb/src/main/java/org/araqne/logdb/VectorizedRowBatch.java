package org.araqne.logdb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.expr.VectorizedExpression;

public class VectorizedRowBatch {
	public boolean selectedInUse;
	public int[] selected;
	public int size;
	public Map<String, Object> data;

	public int getMaxSize() {
		if (selectedInUse) {
			return selected[size - 1] + 1;
		} else {
			return size;
		}
	}

	public VectorizedRowBatch copy() {
		VectorizedRowBatch c = new VectorizedRowBatch();
		c.size = size;
		if (selectedInUse) {
			c.selected = Arrays.copyOf(selected, size);
			c.selectedInUse = true;
		}

		c.data = new HashMap<String, Object>();
		for (String key : data.keySet()) {
			Object[] array = (Object[]) data.get(key);
			c.data.put(key, deepCopy(array));
		}
		return c;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Object[] deepCopy(Object[] array) {
		Object[] c = new Object[array.length];
		for (int i = 0; i < array.length; i++) {
			Object o = array[i];
			if (o instanceof Map) {
				o = Row.clone((Map) o);
			} else if (o instanceof List) {
				o = Row.clone((List) o);
			}
			c[i] = o;
		}

		return c;
	}

	public Object[] eval(Expression expr) {
		if (expr instanceof VectorizedExpression) {
			return ((VectorizedExpression) expr).eval(this);
		} else {
			Object[] values = new Object[size];
			if (selectedInUse) {
				for (int i = 0; i < size; i++) {
					int p = selected[i];
					values[i] = expr.eval(row(p));
				}
			} else {
				for (int i = 0; i < size; i++) {
					values[i] = expr.eval(row(i));
				}
			}
			return values;
		}
	}

	public Object evalOne(Expression expr, int i) {
		if (expr instanceof VectorizedExpression) {
			return ((VectorizedExpression) expr).evalOne(this, i);
		} else {
			return expr.eval(row(i));
		}
	}

	public RowBatch toRowBatch() {
		Row[] rows = new Row[size];
		for (int i = 0; i < size; i++)
			rows[i] = new Row();

		if (selectedInUse) {
			for (String key : data.keySet()) {
				Object[] array = (Object[]) data.get(key);
				for (int i = 0; i < size; i++) {
					int p = selected[i];
					Object val = array[p];
					if (val != null)
						rows[i].put(key, val);
				}
			}
		} else {
			for (String key : data.keySet()) {
				Object[] array = (Object[]) data.get(key);
				for (int i = 0; i < size; i++) {
					Object val = array[i];
					if (val != null)
						rows[i].put(key, val);
				}
			}
		}

		RowBatch rowBatch = new RowBatch();
		rowBatch.size = size;
		rowBatch.rows = rows;
		return rowBatch;
	}

	// converter
	public Row row(int i) {
		if (i >= size)
			return null;

		Map<String, Object> m = new HashMap<String, Object>();
		for (String key : data.keySet()) {
			Object value = ((Object[]) data.get(key))[i];
			if (value != null)
				m.put(key, value);
		}

		return new Row(m);
	}
}
