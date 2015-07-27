package org.araqne.logdb.cep;

public interface EventClockCallback {
	void onRemove(EventKey key, EventClockItem value, String host, EventCause expire);
}
