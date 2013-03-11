/*
 * Copyright 2010 NCHOVY
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;

@Component(name = "delimiter-parser-factory")
@Provides
public class DelimiterParserFactory implements LogParserFactory {

	private static final String DELIMITER_TARGET = "delimiter_target";
	private static final String DELIMITER = "delimiter";
	private static final String COLUMN_HEADERS = "column_headers";
	private static final String INCLUDE_DELIMITER_TARGET = "include_delimiter_target";

	@Override
	public String getName() {
		return DELIMITER;
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH);
	}

	@Override
	public String getDisplayName(Locale locale) {
		return "delimiter parser";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH);
	}

	@Override
	public String getDescription(Locale locale) {
		return "devide a string into tokens based on the given delimiter and column names";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		List<LoggerConfigOption> options = new ArrayList<LoggerConfigOption>();
		{
			Map<Locale, String> displayNames = new HashMap<Locale, String>();
			Map<Locale, String> descriptions = new HashMap<Locale, String>();
			displayNames.put(Locale.ENGLISH, DELIMITER);
			descriptions.put(Locale.ENGLISH, "delimiter character");
			options.add(new StringConfigType(DELIMITER, displayNames, descriptions, true));
		}
		{
			Map<Locale, String> displayNames = new HashMap<Locale, String>();
			Map<Locale, String> descriptions = new HashMap<Locale, String>();
			displayNames.put(Locale.ENGLISH, "column headers");
			descriptions.put(Locale.ENGLISH, "separated by comma");
			options.add(new StringConfigType(COLUMN_HEADERS, displayNames, descriptions, false));
		}
		{
			Map<Locale, String> displayNames = new HashMap<Locale, String>();
			Map<Locale, String> descriptions = new HashMap<Locale, String>();
			displayNames.put(Locale.ENGLISH, "delimiter target field");
			descriptions.put(Locale.ENGLISH, "delimiter target field name");
			options.add(new StringConfigType(DELIMITER_TARGET, displayNames, descriptions, false));
		}
		{
			Map<Locale, String> displayNames = new HashMap<Locale, String>();
			Map<Locale, String> descriptions = new HashMap<Locale, String>();
			displayNames.put(Locale.ENGLISH, "include delimiter target");
			descriptions.put(Locale.ENGLISH, "return also delimiter target field (true or false)");
			options.add(new StringConfigType(INCLUDE_DELIMITER_TARGET, displayNames, descriptions, false));
		}

		return options;
	}

	@Override
	public LogParser createParser(Properties config) {
		String delimiter = config.getProperty(DELIMITER);
		if (delimiter == null)
			delimiter = " ";

		String[] columnHeaders = null;
		String h = config.getProperty(COLUMN_HEADERS);
		if (h != null)
			columnHeaders = h.split(",");

		boolean includeDelimiterTarget = false;
		if (config.containsKey(INCLUDE_DELIMITER_TARGET)) {
			String s = config.getProperty(INCLUDE_DELIMITER_TARGET);
			includeDelimiterTarget = (s != null && Boolean.parseBoolean(s));
		}

		String delimiterTarget = config.getProperty(DELIMITER_TARGET);
		if (delimiterTarget == null)
			delimiterTarget = "line";

		return new DelimiterParser(delimiter, columnHeaders, delimiterTarget, includeDelimiterTarget);
	}
}
