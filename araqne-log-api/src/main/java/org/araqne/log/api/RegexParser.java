/*
 * Copyright 2013 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.log.api;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexParser extends V1LogParser {
	private final String field;
	private final Pattern p;
	private final String[] names;
	private final boolean includeOriginalField;
	private final Matcher matcher;

	public RegexParser(String field, Pattern p, String[] names) {
		this(field, p, names, false);
	}

	public RegexParser(String field, Pattern p, String[] names, boolean includeOriginalField) {
		this.field = field != null ? field : "line";
		this.p = p;
		this.names = names;
		this.includeOriginalField = includeOriginalField;
		this.matcher = p.matcher("");
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		Map<String, Object> m = new HashMap<String, Object>();
		String s = (String) params.get(field);

		matcher.reset(s);
		while (matcher.find())
			for (int i = 0; i < matcher.groupCount(); i++) {
				if (m.get(names[i]) == null)
					m.put(names[i], matcher.group(i + 1));
			}

		if (includeOriginalField)
			m.put(field, s);

		return m;
	}

	@Override
	public String toString() {
		return "regex parser: " + p.pattern();
	}
}
