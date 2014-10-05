package org.araqne.log.api;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;

@Component(name = "daily-directory-watch-logger-factory")
@Provides
public class DailyRollingDirectoryWatchLoggerFactory extends AbstractLoggerFactory {

	@Override
	public String getName() {
		return "daily-dirwatch";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "일자별 디렉터리";
		return "Daily Rolling Directory Watcher";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINESE);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "일자별로 생성되는 디렉터리를 순회하면서 파일 이름 패턴과 일치하는 모든 텍스트 로그 파일을 수집합니다.";
		return "Traverse daily rolling directories and collect matching text log files";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINA);
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {

		LoggerConfigOption period = new MutableIntegerConfigType("period", t("Monitoring period", "모니터링 기간"), t(
				"Period in days for watch file changes. 0 for disable realtime monitoring.",
				"실시간으로 파일 변화를 모니터링할 기간. 0으로 설정 시 실시간 수집을 비활성화합니다."), true);

		LoggerConfigOption basePath = new MutableStringConfigType("base_path", t("Directory path", "디렉터리 경로", "ディレクトリ経路", "目录"),
				t("Base log file directory path", "로그 파일을 수집할 대상 디렉터리 경로", "ログファイルを収集する対象ディレクトリ経路", "要采集的日志文件所在目录"), true);

		LoggerConfigOption fileNamePattern = new MutableStringConfigType("filename_pattern", t("Filename pattern", "파일이름 패턴",
				"ファイルなパータン", "文件名模式"), t("Regular expression to match log file name", "대상 로그 파일을 선택하는데 사용할 정규표현식",
				"対象ログファイルを選ぶとき使う正規表現", "用于筛选文件的正则表达式"), true);

		LoggerConfigOption dirDatePattern = new MutableStringConfigType("dir_date_pattern", t("Directory date pattern",
				"디렉터리 날짜 정규표현식"),
				t("Regular expression for extracting date from directory name", "디렉터리 이름에서 날짜를 추출하는데 사용할 정규표현식"), false);

		LoggerConfigOption dirDateFormat = new MutableStringConfigType("dir_date_format",
				t("Directory date format", "디렉터리 날짜 포맷"), t(
						"Date format for parsing date from directory name. yyyyMMdd by default",
						"디렉터리 이름에서 날짜를 파싱하는데 사용할 포맷. 미설정 시 yyyyMMdd를 기본값으로 사용합니다."), false);

		LoggerConfigOption oldDirScanFrom = new MutableStringConfigType("old_dir_scan_from", t("Begin date for old log scan",
				"과거 로그 수집 시작 일자"), t("Use yyyyMMdd format. Old log scan will be disabled if not specified",
				"yyyyMMdd 포맷으로 시작 일자를 지정합니다. 미설정 시 과거 로그 수집이 비활성화됩니다."), false);

		LoggerConfigOption oldDirScanTo = new MutableStringConfigType("old_dir_scan_to", t("End date for old log scan",
				"과거 로그 수집 끝 일자"), t("Use yyyyMMdd format. Old log scan will be disabled if not specified",
				"yyyyMMdd 포맷으로 끝 일자를 지정합니다. 미설정 시 과거 로그 수집이 비활성화됩니다."), false);

		LoggerConfigOption datePattern = new MutableStringConfigType("date_pattern", t("Date pattern", "날짜 정규표현식", "日付正規表現",
				"日期正则表达式"), t("Regular expression to match date and time strings", "날짜 및 시각을 추출하는데 사용할 정규표현식", "日付と時刻を解析する正規表現",
				"用于提取日期及时间的正则表达式"), false);

		LoggerConfigOption dateFormat = new MutableStringConfigType("date_format", t("Date format", "날짜 포맷", "日付フォーマット", "日期格式"),
				t("Date format to parse date and time strings. e.g. yyyy-MM-dd HH:mm:ss",
						"날짜 및 시각 문자열을 파싱하는데 사용할 포맷. 예) yyyy-MM-dd HH:mm:ss", "日付と時刻を解析するフォーマット。例) yyyy-MM-dd HH:mm:ss",
						"用于解析日期及时间字符串的格式。 示例) yyyy-MM-dd HH:mm:ss"), false);

		LoggerConfigOption dateLocale = new MutableStringConfigType("date_locale", t("Date locale", "날짜 로케일", "日付ロケール", "日期区域"),
				t("date locale, e.g. en", "날짜 로케일, 예를 들면 ko", "日付ロケール。例えばjp", "日期区域， 例如 zh"), false);

		LoggerConfigOption timezone = new MutableStringConfigType("timezone", t("Time zone", "시간대", "時間帯", "时区"), t(
				"time zone, e.g. America/New_york ", "시간대, 예를 들면 KST 또는 Asia/Seoul", "時間帯。例えばJSTまたはAsia/Tokyo",
				"时区，例如 Asia/Beijing"), false);

		LoggerConfigOption newlogRegex = new MutableStringConfigType("newlog_designator", t("Regex for first line", "로그 시작 정규식",
				"ログ始め正規表現", "日志起始正则表达式"), t("Regular expression to determine whether the line is start of new log. "
				+ "if a line does not matches, the line will be merged to prev line.).",
				"새 로그의 시작을 인식하기 위한 정규식(매칭되지 않는 경우 이전 줄에 병합됨)", "新しいログの始まりを認識する正規表現 (マッチングされない場合は前のラインに繋げる)",
				"用于识别日志起始位置的正则表达式(如没有匹配项，则合并到之前日志)"), false);

		LoggerConfigOption newlogEndRegex = new MutableStringConfigType("newlog_end_designator", t("Regex for last line",
				"로그 끝 정규식", "ログ終わり正規表現", "日志结束正则表达式"), t("Regular expression to determine whether the line is end of new log."
				+ "(if a line does not matches, the line will be merged to prev line.).",
				"로그의 끝을 인식하기 위한 정규식(매칭되지 않는 경우 이전 줄에 병합됨)", "ログの終わりを認識する正規表現 (マッチングされない場合は前のラインに繋げる)",
				"用于识别日志结束位置地正则表达式(如没有匹配项，则合并到之前日志)"), false);

		LoggerConfigOption charset = new MutableStringConfigType("charset", t("Charset", "문자 집합", "文字セット", "字符集"), t(
				"charset encoding", "텍스트 파일의 문자 인코딩 방식", "テキストファイルの文字エンコーディング方式", "文本文件的字符编码方式"), false);

		LoggerConfigOption fileTag = new MutableStringConfigType("file_tag", t("Filename Tag", "파일네임 태그", "ファイル名タグ", "文件名标记"), t(
				"Field name for filename tagging", "파일명을 태깅할 필드 이름", "ファイル名をタギングするフィールド名", "要进行文件名标记的字段"), false);

		return Arrays.asList(period, basePath, fileNamePattern, dirDatePattern, dirDateFormat, oldDirScanFrom, oldDirScanTo,
				datePattern, dateFormat, dateLocale, timezone, newlogRegex, newlogEndRegex, charset, fileTag);
	}

	private Map<Locale, String> t(String enText, String koText) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, enText);
		m.put(Locale.KOREAN, koText);
		return m;
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
		return new DailyRollingDirectoryWatchLogger(spec, this);
	}
}
