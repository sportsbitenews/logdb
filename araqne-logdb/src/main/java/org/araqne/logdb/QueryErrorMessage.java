package org.araqne.logdb;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class QueryErrorMessage {
	private Map<Locale, String> templates = new HashMap<Locale, String>();

	public QueryErrorMessage(String en, String ko) {
		this(en, ko, null);
	}

	public QueryErrorMessage(String en, String ko, String zh) {
		templates.put(Locale.ENGLISH, en);
		templates.put(Locale.KOREAN, ko);

		if (zh != null)
			templates.put(Locale.CHINESE, zh);
	}

	public String format(Locale locale, Map<String, String> params) {
		String template = templates.get(locale);
		if (template == null) 
			template = templates.get(Locale.ENGLISH);

		if (template == null)
			return null;

		if (params == null)
			return template;
		
		// while     param replace
		// template.replaceAll(regex, replacement)
		for (Map.Entry<String, String> entry : params.entrySet()) {
			template = template.replace("[" + entry.getKey() + "]", entry.getValue());
		}

		//포맷 UI담당자랑 협의 후 수정
		return template;
	}
}
