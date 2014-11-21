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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;

@Component(name = "field-mapping-parser-factory")
@Provides
public class FieldMappingParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "fieldmapper";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "필드 이름 변경";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "フィルド名変更";
		if(locale != null && locale.equals(Locale.CHINESE))
			return "修改字段名称";
		return "Field mapper";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "필드 이름을 변경합니다.";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "フィルドの名前を変更します。";
		if(locale != null && locale.equals(Locale.CHINESE))
			return "修改字段名称。";
		return "Replace original field name by specified name.";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption mappings = new StringConfigType("mappings", t("Field mappings", "필드 이름 변환 목록", "フィルド名変更リスト", "字段名称转换列表"), t(
				"Comma separated from=to field mappings", "쉼표로 구분되는 원본필드=변경필드 이름 목록", "コンマで区分される元＝変更フィルド名リスト", "格式：原始字段名称1=变更字段名称1,原始字段名称2=变更字段名称2,…"), true);
		return Arrays.asList(mappings);
	}

	private Map<Locale, String> t(String enText, String koText, String jpText, String cnText) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, enText);
		m.put(Locale.KOREAN, koText);
		m.put(Locale.JAPANESE, jpText);
		m.put(Locale.CHINESE, cnText);
		return m;
	}

	@Override
	public LogParser createParser(Map<String, String> config) {
		return new FieldMappingParser(config);
	}
}
