package org.araqne.log.api;

import java.util.Locale;
import java.util.Map;

public class SourceLoggerConfigType extends AbstractConfigType {
	public SourceLoggerConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions,
			boolean required) {
		super(name, displayNames, descriptions, required);
	}

	public String getType() {
		return "source-logger";
	}

	public Object parse(String value) {
		return value;
	}
}
