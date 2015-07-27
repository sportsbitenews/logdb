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
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;

@Component(name = "directory-watch-logger-factory")
@Provides
public class DirectoryWatchLoggerFactory extends AbstractLoggerFactory {

	@Override
	public String getName() {
		return "dirwatch";
	}
	
	@Override
	public List<Locale> getLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDisplayGroup(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "로컬";
		return "Local";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "디렉터리 와처";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "ディレクトリウォッチャー";
		if(locale != null && locale.equals(Locale.CHINESE))
			return "目录监控";

		return "Directory Watcher";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "지정된 디렉터리에서 파일이름 패턴과 일치하는 모든 텍스트 로그 파일을 수집합니다.";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "指定されたディレクトリでファイル名パータンと一致するすべてのテキストログファイルを収集します。";
		if(locale != null && locale.equals(Locale.CHINESE))
			return "从指定目录中，采集与文件名模式匹配的所有文本文件。";

		return "Collect all text log files in specified directory";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {

		LoggerConfigOption basePath = new MutableStringConfigType("base_path", t("Directory path", "디렉터리 경로", "ディレクトリ経路","目录"),
				t("Base log file directory path", "로그 파일을 수집할 대상 디렉터리 경로", "ログファイルを収集する対象ディレクトリ経路","要采集的日志文件所在目录"), true);

		LoggerConfigOption fileNamePattern = new MutableStringConfigType("filename_pattern", 
				t("Filename pattern", "파일이름 패턴","ファイルなパータン", "文件名模式"), 
				t("Regular expression to match log file name","대상 로그 파일을 선택하는데 사용할 정규표현식", 
						"対象ログファイルを選ぶとき使う正規表現", "用于筛选文件的正则表达式"), true);

		LoggerConfigOption datePattern = new MutableStringConfigType("date_pattern",
				t("Date pattern", "날짜 정규표현식", "日付正規表現","日期正则表达式"), 
				t("Regular expression to match date and time strings", "날짜 및 시각을 추출하는데 사용할 정규표현식", 
						"日付と時刻を解析する正規表現","用于提取日期及时间的正则表达式"), false);

		LoggerConfigOption dateFormat = new MutableStringConfigType("date_format", 
				t("Date format", "날짜 포맷", "日付フォーマット","日期格式"), 
				t("date format to parse date and time strings. e.g. yyyy-MM-dd HH:mm:ss",
				"날짜 및 시각 문자열을 파싱하는데 사용할 포맷. 예) yyyy-MM-dd HH:mm:ss", 
				"日付と時刻を解析するフォーマット。例) yyyy-MM-dd HH:mm:ss",
				"用于解析日期及时间字符串的格式。 示例) yyyy-MM-dd HH:mm:ss"), false);

		LoggerConfigOption dateLocale = new MutableStringConfigType("date_locale", 
				t("Date locale", "날짜 로케일", "日付ロケール", "日期区域"), 
				t("Date locale, e.g. en", "날짜 로케일, 예를 들면 ko", 
						"日付ロケール。例えばjp", "日期区域， 例如 zh"), false);

		LoggerConfigOption timezone = new MutableStringConfigType("timezone", 
				t("Time zone", "시간대","時間帯","时区"), 
				t("Time zone, e.g. America/New_york ", "시간대, 예를 들면 KST 또는 Asia/Seoul",
						"時間帯。例えばJSTまたはAsia/Tokyo","时区，例如 Asia/Beijing"), false);

		LoggerConfigOption newlogRegex = new MutableStringConfigType("newlog_designator",
				t("Regex for first line", "로그 시작 정규식", "ログ始め正規表現","日志起始正则表达式"), 
				t("Regular expression to determine whether the line is start of new log. "
						+ "if a line does not matches, the line will be merged to prev line.).",
						"새 로그의 시작을 인식하기 위한 정규식(매칭되지 않는 경우 이전 줄에 병합됨)", 
						"新しいログの始まりを認識する正規表現 (マッチングされない場合は前のラインに繋げる)",
						"用于识别日志起始位置的正则表达式(如没有匹配项，则合并到之前日志)"), false);

		LoggerConfigOption newlogEndRegex = new MutableStringConfigType("newlog_end_designator", 
				t("Regex for last line", "로그 끝 정규식", "ログ終わり正規表現", "日志结束正则表达式"),
				t("Regular expression to determine whether the line is end of new log."
						+ "(if a line does not matches, the line will be merged to prev line.).",
						"로그의 끝을 인식하기 위한 정규식(매칭되지 않는 경우 이전 줄에 병합됨)", 
						"ログの終わりを認識する正規表現 (マッチングされない場合は前のラインに繋げる)",
						"用于识别日志结束位置地正则表达式(如没有匹配项，则合并到之前日志)"
						), false);

		LoggerConfigOption charset = new MutableStringConfigType("charset",
				t("Charset", "문자 집합", "文字セット","字符集"),
				t("charset encoding", "텍스트 파일의 문자 인코딩 방식", 
						"テキストファイルの文字エンコーディング方式", "文本文件的字符编码方式"), false);

		return Arrays
				.asList(basePath, fileNamePattern, datePattern, dateFormat, dateLocale, timezone,  newlogRegex, newlogEndRegex, charset);
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
		return new DirectoryWatchLogger(spec, this);
	}

	@Override
	public void deleteLogger(String namespace, String name) {
		super.deleteLogger(namespace, name);

		// delete lastpos file
		File dataDir = new File(System.getProperty("araqne.data.dir"), "araqne-log-api");
		File f = new File(dataDir, "dirwatch-" + name + ".lastlog");
		f.delete();
	}

}
