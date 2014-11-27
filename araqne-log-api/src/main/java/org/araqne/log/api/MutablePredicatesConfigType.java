package org.araqne.log.api;

import java.util.Locale;
import java.util.Map;

public class MutablePredicatesConfigType extends PredicatesConfigType implements Mutable {
	public MutablePredicatesConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions,
			boolean required) {
		super(name, displayNames, descriptions, required);
	}
}
