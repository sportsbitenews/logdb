package org.araqne.log.api;

import java.util.Locale;
import java.util.Map;

public class MutableStringConfigType extends StringConfigType implements Mutable {
	public MutableStringConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions,
			boolean required) {
		super(name, displayNames, descriptions, required);
	}

	public MutableStringConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions,
			boolean required, Map<Locale, String> defaultValues) {
		super(name, displayNames, descriptions, required, defaultValues);
	}

	public MutableStringConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions,
			boolean required, Subtype subtype) {
		super(name, displayNames, descriptions, required, subtype);
	}

	public MutableStringConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions,
			boolean required, Map<Locale, String> defaultValues, Subtype subtype) {
		super(name, displayNames, descriptions, required, defaultValues, subtype);
	}

}
