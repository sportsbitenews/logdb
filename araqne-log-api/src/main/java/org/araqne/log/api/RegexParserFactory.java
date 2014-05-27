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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;

@Component(name = "regex-parser-factory")
@Provides
public class RegexParserFactory implements LogParserFactory {

	@Override
	public String getName() {
		return "regex";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "정규표현식";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "正規表現";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "正则表达式";
		return "Regular Expression";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "정규표현식을 이용하여 로그를 파싱합니다.";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "正規表現を使ってログを解析します。";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "利用正则表达式解析日志。";
		return "parse log using regular expression";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		List<LoggerConfigOption> options = new ArrayList<LoggerConfigOption>();
		options.add(new StringConfigType("regex", t("regex", "정규표현식", "正規表現", "正则表达式"), t("regular expression with placeholder",
				"필드 이름이 포함된 정규표현식", "フィールド名を含めている正規表現", "包含字段名称的正则表达式"), true));
		options.add(new StringConfigType("field", t("field", "대상 필드", "対象フィールド", "目标字段"), t(
				"parse target field. 'line' field by default", "정규표현식을 적용할 필드, 미설정 시 기본값은 line", 
				"正規表現を適用するフィールド。未設定の場合はline",
				"要应用正则表达式的字段, 默认值为line。"),
				false));
		options.add(new StringConfigType("include_original_field", t("include original field", "원본 필드 포함", "元フィールド込み", "包含原始字段"), t(
				"return also original field (true or false)", 
				"정규표현식으로 파싱된 결과 외에 원본 필드 값도 포함할지 설정합니다. true 혹은 false",
				"正規表現の結果と元のフィールドも含むか設定します。trueかfalse",
				"设置除通过正则表达式解析的结果之外，是否包含原始字段值。True或者false。"), false));
		return options;
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
		String field = config.get("field");
		List<String> names = new ArrayList<String>();

		Pattern placeholder = Pattern.compile("\\(\\?<(.*?)>");
		String regexToken = config.get("regex");

		regexToken = toNonCapturingGroup(regexToken);

		Matcher matcher = placeholder.matcher(regexToken);
		while (matcher.find())
			names.add(matcher.group(1));

		while (true) {
			matcher = placeholder.matcher(regexToken);
			if (!matcher.find())
				break;

			// suppress special meaning of $ and \
			String quoted = Matcher.quoteReplacement("(");
			regexToken = matcher.replaceFirst(quoted);
		}

		boolean includeOriginalField = false;
		String s = config.get("include_original_field");
		if (s != null)
			includeOriginalField = Boolean.parseBoolean(s);

		Pattern p = Pattern.compile(regexToken);
		return new RegexParser(field, p, names.toArray(new String[0]), includeOriginalField);
	}

	private String toNonCapturingGroup(String s) {
		StringBuilder sb = new StringBuilder();

		char last2 = ' ';
		char last = ' ';
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (last2 != '\\' && last == '(' && c != '?')
				sb.append("?:");
			sb.append(c);
			last2 = last;
			last = c;
		}

		return sb.toString();
	}

}
