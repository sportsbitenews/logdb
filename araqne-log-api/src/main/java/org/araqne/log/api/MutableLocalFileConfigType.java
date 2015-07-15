package org.araqne.log.api;

import java.util.Locale;
import java.util.Map;

public class MutableLocalFileConfigType extends LocalFileConfigType implements Mutable {
	public MutableLocalFileConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions,
			boolean required) {
		super(name, displayNames, descriptions, required);
	}

	public MutableLocalFileConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions,
			boolean required, Map<Locale, String> defaultValues) {
		super(name, displayNames, descriptions, required, defaultValues);
	}
}
