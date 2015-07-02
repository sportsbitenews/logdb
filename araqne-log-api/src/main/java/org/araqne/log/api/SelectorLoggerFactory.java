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
import org.apache.felix.ipojo.annotations.Requires;

@Component(name = "selector-logger-factory")
@Provides
public class SelectorLoggerFactory extends AbstractLoggerFactory {
	private static final String OPT_SOURCE_LOGGER = "source_logger";
	private static final String OPT_PATTERN = "pattern";

	@Requires
	private LoggerRegistry loggerRegistry;

	@Override
	public String getName() {
		return "selector";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "로그 선택자";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "ログセレクター";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "数据筛选器";
		return "Log Selector";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "다른 로거로부터 패턴 매칭되는 특정 로그들만 수집합니다.";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "他のロガーからパターンマッチングされる特定ログだけ収集します。";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "从其他数据采集器提取符合指定特征的数据。";
		return "Select logs from logger using text matching";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption loggerName = new MutableSourceLoggerConfigType(OPT_SOURCE_LOGGER, t("Source logger name", "원본 로거 이름",
				"元ロガー名", "源数据采集器"), t("Full name of data source logger", "네임스페이스를 포함한 원본 로거 이름", "ネームスペースを含む元ロガー名",
				"包含名字空间的源数据采集器名称"), true);
		LoggerConfigOption pattern = new MutableStringConfigType(OPT_PATTERN, t("Text pattern", "텍스트 패턴", "テキストパターン", "文本模式"), t(
				"Text pattern to match", "매칭할 대상 문자열", "マッチングする対象文字列", "要匹配的字符串"), true);
		return Arrays.asList(loggerName, pattern);
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
	protected Logger createLogger(LoggerSpecification spec) {
		String fullName = spec.getNamespace() + "\\" + spec.getName();
		String sourceFullName = (String) spec.getConfig().get("source_logger");

		Logger logger = new SelectorLogger(spec, this, loggerRegistry);
		loggerRegistry.addDependency(fullName, sourceFullName);
		return logger;
	}

	@Override
	public void deleteLogger(String namespace, String name) {
		Logger logger = loggerRegistry.getLogger(namespace, name);
		super.deleteLogger(namespace, name);

		String sourceFullname = (String) logger.getConfigs().get("source_logger");
		loggerRegistry.removeDependency(logger.getFullName(), sourceFullname);
	}

}
