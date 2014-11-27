package org.araqne.log.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONConverter;

public class PredicatesConfigType extends AbstractConfigType {

	public PredicatesConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions, boolean required) {
		super(name, displayNames, descriptions, required);
	}

	@Override
	public String getType() {
		return "predicates";
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object parse(String value) {
		try {
			List<PredicateOption> predicates = new ArrayList<PredicateOption>();
			List<Object> l = (List<Object>) JSONConverter.parse(new JSONArray(value));
			for (Object i : l) {
				List<Object> items = (List<Object>) i;
				String cond = items.get(0).toString();
				String val = items.get(1).toString();
				predicates.add(new PredicateOption(cond, val));
			}

			return predicates;
		} catch (Throwable t) {
			throw new IllegalArgumentException("invalid predicates json", t);
		}
	}
}
