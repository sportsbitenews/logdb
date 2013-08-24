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

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;

@Component(name = "rotation-file-logger-factory")
@Provides
public class RotationFileLoggerFactory extends AbstractLoggerFactory {

	@Override
	public String getName() {
		return "rotation";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "로테이션 로그 파일";
		return "Rotation Log File";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "일정 주기마다 다른 경로에 백업 후 삭제하고 다시 쓰는 로그 파일을 수집합니다.";
		return "collect rotation text log file";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption filePath = new StringConfigType("file_path", t("File Path", "파일 경로"), t("Log file path", "텍스트 로그 파일의 절대 경로"),
				true, t("/var/log/"));

		LoggerConfigOption datePattern = new StringConfigType("date_pattern", t("Date Pattern", "날짜 정규식"), t(
				"Regex for date extraction", "날짜 문자열 추출에 사용되는 정규표현식"), false, t(null));

		LoggerConfigOption dateFormat = new StringConfigType("date_format", t("Date Format", "날짜 패턴"), t("Date pattern of log file",
				"날짜 파싱에 필요한 패턴 (예시: yyyy-MM-dd HH:mm:ss)"), false, t("MMM dd HH:mm:ss"));

		LoggerConfigOption dateLocale = new StringConfigType("date_locale", t("Date Locale", "날짜 로케일"), t("Date locale of log file",
				"날짜 문자열의 로케일. 가령 날짜 패턴의 MMM 지시어은 영문 로케일에서 Jan으로 인식됩니다."), false, t("en"));

		LoggerConfigOption charset = new StringConfigType("charset", t("Charset", "문자 집합"), t("Charset", "문자 집합. 기본값 UTF-8"), false,
				t("utf-8"));

		LoggerConfigOption logBeginRegex = new StringConfigType("begin_regex",
				t("Log begin regex", "로그 시작 구분 정규식"),
				t("Regular expression to determine whether the line is start of new log."
						+ "(if a line does not matches, the line will be merged to prev line.).",
						"새 로그의 시작을 인식하기 위한 정규식(매칭되지 않는 경우 이전 줄에 병합됨)"), false);

		LoggerConfigOption logEndRegex = new StringConfigType("end_regex",
				t("Log end regex", "로그 끝 구분 정규식"),
				t("Regular expression to determine whether the line is end of new log."
						+ "(if a line does not matches, the line will be merged to prev line.).",
						"로그의 끝을 인식하기 위한 정규식(매칭되지 않는 경우 이전 줄에 병합됨)"), false);

		return Arrays.asList(filePath, charset, datePattern, dateFormat, dateLocale, logBeginRegex, logEndRegex);
	}

	private Map<Locale, String> t(String text) {
		return t(text, text);
	}

	private Map<Locale, String> t(String enText, String koText) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, enText);
		m.put(Locale.KOREAN, koText);
		return m;
	}

	@Override
	protected Logger createLogger(LoggerSpecification spec) {
		return new RotationFileLogger(spec, this);
	}

	@Override
	public void deleteLogger(String namespace, String name) {
		super.deleteLogger(namespace, name);

		// delete lastpos file
		File dataDir = new File(System.getProperty("araqne.data.dir"), "araqne-log-api");
		File f = new File(dataDir, "rotation-" + name + ".lastlog");
		f.delete();
	}

}
