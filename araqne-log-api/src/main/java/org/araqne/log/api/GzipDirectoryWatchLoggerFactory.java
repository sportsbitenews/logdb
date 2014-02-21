package org.araqne.log.api;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;

@Component(name = "gzip-directory-watch-logger-factory")
@Provides
public class GzipDirectoryWatchLoggerFactory extends AbstractLoggerFactory {

	@Override
	public String getName() {
		return "gzip-dirwatch";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "GZIP 디렉터리 와처";
		return "GZIP Directory Watcher";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "지정된 디렉터리에서 파일이름 패턴과 일치하는 모든 gzip 파일을 수집합니다.";
		return "Collect all gzip files in specified directory";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption basePath = new StringConfigType("base_path", t("Directory path", "디렉터리 경로"), t(
				"Base gzip file directory path", "gzip 파일을 수집할 대상 디렉터리 경로"), true);

		LoggerConfigOption fileNamePattern = new StringConfigType("filename_pattern", t("Filename pattern", "파일이름 패턴"), t(
				"Regular expression to match gzip file name", "대상 gzip 파일을 선택하는데 사용할 정규표현식"), true);

		LoggerConfigOption datePattern = new StringConfigType("date_pattern", t("Date pattern", "날짜 정규표현식"), t(
				"Regular expression to match date and time strings", "날짜 및 시각을 추출하는데 사용할 정규표현식"), false);

		LoggerConfigOption dateFormat = new StringConfigType("date_format", t("Date format", "날짜 포맷"), t(
				"date format to parse date and time strings. e.g. yyyy-MM-dd HH:mm:ss",
				"날짜 및 시각 문자열을 파싱하는데 사용할 포맷. 예) yyyy-MM-dd HH:mm:ss"), false);

		LoggerConfigOption dateLocale = new StringConfigType("date_locale", t("Date locale", "날짜 로케일"), t("date locale, e.g. en",
				"날짜 로케일, 예를 들면 ko"), false);

		LoggerConfigOption newlogRegex = new StringConfigType("newlog_designator",
				t("Regex for first line", "로그 시작 정규식"),
				t("Regular expression to determine whether the line is start of new log."
						+ "(if a line does not matches, the line will be merged to prev line.).",
						"새 로그의 시작을 인식하기 위한 정규식(매칭되지 않는 경우 이전 줄에 병합됨)"), false);

		LoggerConfigOption newlogEndRegex = new StringConfigType("newlog_end_designator",
				t("Regex for last line", "로그 끝 정규식"),
				t("Regular expression to determine whether the line is end of new log."
						+ "(if a line does not matches, the line will be merged to prev line.).",
						"로그의 끝을 인식하기 위한 정규식(매칭되지 않는 경우 이전 줄에 병합됨)"), false);

		LoggerConfigOption charset = new StringConfigType("charset", t("Charset", "문자 집합"), t("charset encoding",
				"gzip 압축 해제된 텍스트 파일의 문자 인코딩 방식"), false);

		LoggerConfigOption isDeleteFile = new StringConfigType("is_delete", t("Delete GZIP file",
				"GZIP 파일 삭제 (true 혹은 false)"), t("Delete collected GZIP file", "수집 완료된 GZIP 파일의 삭제 여부"), false);

		return Arrays
				.asList(basePath, fileNamePattern, datePattern, dateFormat, dateLocale, newlogRegex, newlogEndRegex, charset,
						isDeleteFile);
	}

	private Map<Locale, String> t(String enText, String koText) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, enText);
		m.put(Locale.KOREAN, koText);
		return m;
	}

	@Override
	protected Logger createLogger(LoggerSpecification spec) {
		return new GzipDirectoryWatchLogger(spec, this);
	}
}
