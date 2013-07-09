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
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "정규식 필터";
		return "regex filter";
	}

	@Override
	public List<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDescription(Locale locale) {
		return "pass or drop logs by regex match result";
	}

	@Override
	public List<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption pattern = new StringConfigType("pattern", t("regex", "정규식"), t("regex for regex filtering",
				"로그를 필터링하는데 사용할 정규표현식"), true);
		LoggerConfigOption inverse = new StringConfigType("inverse", t("inverse match", "매칭결과 반전"), t(
				"inverse regex match result, false by default", "정규표현식 매칭 결과를 반전합니다. 기본값은 false 입니다."), false);
		return Arrays.asList(pattern, inverse);
	}

	private Map<Locale, String> t(String enText, String koText) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, enText);
		m.put(Locale.KOREAN, koText);
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
