package org.araqne.logdb;

import java.util.HashMap;
import java.util.Map;

public class VectorizedRowBatch {
	public boolean selectedInUse;
	public int[] selected;
	public int size;
	public Map<String, Object> data;

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
