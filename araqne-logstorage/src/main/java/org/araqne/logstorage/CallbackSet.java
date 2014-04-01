package org.araqne.logstorage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class CallbackSet {
	private ConcurrentMap<Class<?>, CopyOnWriteArraySet<?>> callbackMap = new ConcurrentHashMap<Class<?>, CopyOnWriteArraySet<?>>();

	@SuppressWarnings("unchecked")
	public <T> CopyOnWriteArraySet<T> get(Class<T> class1) {
		CopyOnWriteArraySet<?> result = callbackMap.get(class1);
		if (result == null) {
			result = new CopyOnWriteArraySet<T>();
			CopyOnWriteArraySet<?> concensus = callbackMap.putIfAbsent(class1, result);
			if (concensus != null)
				result = concensus;
		}
		return (CopyOnWriteArraySet<T>) result;
	}
}