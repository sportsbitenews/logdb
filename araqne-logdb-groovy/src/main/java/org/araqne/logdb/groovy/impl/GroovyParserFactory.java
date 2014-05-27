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
		if(locale != null && locale.equals(Locale.CHINESE))
			return "Groovy";
		return "Groovy";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINESE);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "그루비 파서 스크립트";
		if(locale != null && locale.equals(Locale.CHINESE))
			return "Groovy Parser脚本";
		return "Groovy parser script";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINESE);
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption scriptName = new StringConfigType("script_name", t("Script Name", "스크립트 이름", "脚本名称"), t(
				"Script file name except .groovy extension", ".groovy 확장자를 제외한 스크립트 파일 이름", "除.groovy扩展名之外的脚本文件名"), true);
		return Arrays.asList(scriptName);
	}

	private Map<Locale, String> t(String en, String ko, String cn) {
		HashMap<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, en);
		m.put(Locale.KOREAN, ko);
		m.put(Locale.CHINESE, cn);
		return m;
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		String scriptName = configs.get("script_name");
		return parserScriptRegistry.newScript(scriptName);
	}
}
