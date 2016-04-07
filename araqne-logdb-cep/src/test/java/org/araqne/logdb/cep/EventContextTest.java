package org.araqne.logdb.cep;
import static org.junit.Assert.assertEquals;

import org.araqne.logdb.Row;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventKey;
import org.junit.Test;

public class EventContextTest {

	@Test
	public void cloneTest() {
		EventKey key = new EventKey("topic", "key", "host");
		EventContext value = new EventContext(key, 0L, 0L, 0L, 10);
		Row row1 = new Row();
		row1.put("1", "row");
		value.getCounter().set(12);
		value.addRow(new Row());
		value.addRow(row1);
		value.setVariable("intVar", 1);
		value.setVariable("stringVar", "string");

		EventContext cloned = value.clone();
		assertEquals(value, cloned);
	}

}
