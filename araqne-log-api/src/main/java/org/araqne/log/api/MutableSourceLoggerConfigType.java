package org.araqne.log.api;

import java.util.Locale;
import java.util.Map;

public class MutableSourceLoggerConfigType extends SourceLoggerConfigType implements Mutable {
	public MutableSourceLoggerConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions,
			boolean required) {
		super(name, displayNames, descriptions, required);
	}
}
