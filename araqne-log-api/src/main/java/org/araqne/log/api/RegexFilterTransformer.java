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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @since 2.3.1
 * @author xeraph
 *
 */
public class RegexFilterTransformer implements LogTransformer {
	private final LogTransformerFactory factory;
	private final Matcher matcher;

	// inverse match
	private final boolean inverse;

	public RegexFilterTransformer(LogTransformerFactory factory, Map<String, String> config) {
		this.factory = factory;
		Pattern p = Pattern.compile((String) config.get("pattern"));
		this.matcher = p.matcher("");

		String s = config.get("inverse");
		inverse = s == null ? false : Boolean.parseBoolean(s);
	}

	@Override
	public LogTransformerFactory getTransformerFactory() {
		return factory;
	}

	@Override
	public Log transform(Log log) {
		String line = (String) log.getParams().get("line");
		matcher.reset(line);

		if (matcher.find() ^ inverse)
			return log;

		return null;
	}

}
