package org.araqne.logdb.cep;

public interface EventClockCallback {
	void onRemove(EventClockItem value, EventCause expire);
}
