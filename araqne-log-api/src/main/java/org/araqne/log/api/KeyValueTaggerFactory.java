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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;

@Component(name = "key-value-tagger-factory")
@Provides
public class KeyValueTaggerFactory implements LogTransformerFactory {

	@Override
	public String getName() {
		return "keyvalue";
	}

	@Override
	public List<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "키/밸류 태그";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "キー・バリュータグ";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "键/值标记";
		return "key/value tagger";
	}

	@Override
	public List<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "모든 로그에 키/밸류 태그를 추가 합니다.";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "すべてのログにキー・バリュータグを追加します。";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "向所有数据添加键/值标记。";
		return "add key/value tag to every logs";
	}

	@Override
	public List<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption tags = new StringConfigType("tags", t("tags", "태그 목록", "タグリスト", "标记列表"), t("comma separated key=value pairs",
				"쉼표로 구분된 키=값 목록", "コンマで区分されているキー＝バリュリスト", "通过正则表达式过滤数据。"), true);
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
	public LogTransformer newTransformer(Map<String, String> config) {
		return new KeyValueTagger(this, config);
	}

	@Override
	public String toString() {
		return "add key=value pairs to original log";
	}
}
