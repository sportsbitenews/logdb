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
import java.util.Map.Entry;

public class FieldMappingParser implements LogParser {
	private Map<String, String> mappings = new HashMap<String, String>();

	public FieldMappingParser(Map<String, String> config) {
		String line = config.get("mappings");
		String[] tokens = line.split(",");
		for (String token : tokens) {
			int p = token.indexOf('=');
			String from = token.substring(0, p);
			String to = token.substring(p + 1);
			mappings.put(from, to);
		}
	}

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		for (Entry<String, String> mapping : mappings.entrySet()) {
			Object value = params.remove(mapping.getKey());
			params.put(mapping.getValue(), value);
		}
		return params;
	}

}
