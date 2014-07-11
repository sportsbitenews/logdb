package org.araqne.log.api;

import java.util.Locale;
import java.util.Map;

public class MutableIntegerConfigType extends IntegerConfigType implements Mutable {

	public MutableIntegerConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions,
			boolean isRequired, Map<Locale, String> defaultValues) {
		super(name, displayNames, descriptions, isRequired, defaultValues);
		// TODO Auto-generated constructor stub
	}

	public MutableIntegerConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions,
			boolean isRequired) {
		super(name, displayNames, descriptions, isRequired);
		// TODO Auto-generated constructor stub
	}

}
