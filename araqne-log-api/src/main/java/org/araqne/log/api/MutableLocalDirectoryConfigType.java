package org.araqne.log.api;

import java.util.Locale;
import java.util.Map;

public class MutableLocalDirectoryConfigType extends LocalDirectoryConfigType implements Mutable {
	public MutableLocalDirectoryConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions,
			boolean required) {
		super(name, displayNames, descriptions, required);
	}

	public MutableLocalDirectoryConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions,
			boolean required, Map<Locale, String> defaultValues) {
		super(name, displayNames, descriptions, required, defaultValues);
	}
}
