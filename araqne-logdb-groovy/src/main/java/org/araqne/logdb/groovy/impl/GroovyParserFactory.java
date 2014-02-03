/*
 * Copyright 2014 Eediom Inc.
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
package org.araqne.logdb.groovy.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.StringConfigType;
import org.araqne.logdb.groovy.GroovyParserScriptRegistry;

@Component(name = "logdb-groovy-parser-factory")
@Provides
public class GroovyParserFactory extends AbstractLogParserFactory {

	@Requires
	private GroovyParserScriptRegistry parserScriptRegistry;

	@Override
	public String getName() {
		return "groovy";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "그루비";
		return "Groovy";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "그루비 파서 스크립트";
		return "Groovy parser script";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption scriptName = new StringConfigType("script_name", t("Script Name", "스크립트 이름"), t(
				"Script file name except .groovy extension", ".groovy 확장자를 제외한 스크립트 파일 이름"), true);
		return Arrays.asList(scriptName);
	}

	private Map<Locale, String> t(String en, String ko) {
		HashMap<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, en);
		m.put(Locale.KOREAN, ko);
		return m;
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		String scriptName = configs.get("script_name");
		return parserScriptRegistry.newScript(scriptName);
	}
}
