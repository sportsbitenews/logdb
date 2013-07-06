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

public class KeyValueTagger implements LogTransformer {
	private LogTransformerFactory factory;
	private Map<String, String> tagMap = new HashMap<String, String>();

	public KeyValueTagger(LogTransformerFactory factory, Map<String, String> config) {
		this.factory = factory;
		String tags = config.get("tags");
		for (String tag : tags.split(",")) {
			int p = tag.indexOf("=");
			String key = tag.substring(0, p).trim();
			String value = tag.substring(p + 1).trim();
			tagMap.put(key, value);
		}
	}

	@Override
	public LogTransformerFactory getTransformerFactory() {
		return factory;
	}

	@Override
	public Log transform(Log log) {
		log.getParams().putAll(tagMap);
		return log;
	}

}
