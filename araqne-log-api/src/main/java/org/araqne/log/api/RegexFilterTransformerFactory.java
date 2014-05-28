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

/**
 * @since 2.3.1
 * @author xeraph
 * 
 */
@Component(name = "regex-filter-transformer-factory")
@Provides
public class RegexFilterTransformerFactory implements LogTransformerFactory {

	@Override
	public String getName() {
		return "regex-filter";
	}

	@Override
	public List<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "정규식 필터";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "正規表現フィルター";
		if (locale != null && locale != null && locale.equals(Locale.CHINESE))
			return "正则表达式过滤器";
		return "regex filter";
	}

	@Override
	public List<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "설정된 정규표현식 패턴을 포함하는 로그만 저장합니다.";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "設定した正規表現パータンを含むログだけ保存します。";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "只保存符合设置的正则表达式特征的日志。";
		return "pass or drop logs by regex match result";
	}

	@Override
	public List<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption pattern = new StringConfigType("pattern", t("regex", "정규식", "正規表現", "正则表达式"), t("regex for regex filtering",
				"로그를 필터링하는데 사용할 정규표현식", "ログのフィルタリングに使う正規表現", "通过正则表达式过滤数据。"), true);
		LoggerConfigOption inverse = new StringConfigType("inverse", t("inverse match", "매칭결과 반전", "結果反転", "返回匹配结果"), t(
				"inverse regex match result, false by default", "정규표현식 매칭 결과를 반전합니다. 기본값은 false 입니다.",
				"マッチング結果を反転します。基本値はfalseです。", "返回正则表达式匹配结果，默认为false。"), false);
		return Arrays.asList(pattern, inverse);
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
		return new RegexFilterTransformer(this, config);
	}

	@Override
	public String toString() {
		return "filter logs by regular expression";
	}
}
