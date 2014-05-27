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

@Component(name = "tag-parser-factory")
@Provides
public class TagParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "tag";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "태그";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "タグ";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "标记";
		return "Tag";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "지정된 태그를 추가합니다.";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "指定したタグを追加します。";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "添加指定的标记。";
		return "add specified tags";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption tags = new StringConfigType("tags", t("tag list", "태그 목록", "タグリスト", "标记列表"), t(
				"comma separated key=value tags", "쉼표로 구분된 키=값 형태의 태그 목록", 
				"コンマで区分されているキー＝バリュー形のタグリスト", "以逗号分隔的键=值格式的标记列表"), true);
		return Arrays.asList(tags);
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
	public LogParser createParser(Map<String, String> configs) {
		String line = configs.get("tags");

		Map<String, String> tags = new HashMap<String, String>();
		if (!line.trim().isEmpty()) {
			for (String pair : line.split(",")) {
				pair = pair.trim();
				int p = pair.indexOf('=');
				String key = pair.substring(0, p);
				String value = pair.substring(p + 1);
				tags.put(key, value);
			}
		}

		return new TagParser(tags);
	}

}
