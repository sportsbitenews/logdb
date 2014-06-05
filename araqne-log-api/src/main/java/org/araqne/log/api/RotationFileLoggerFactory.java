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
		if (locale != null && locale.equals(Locale.KOREAN))
			return "로테이션 로그 파일";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "ローテーションログファイル";
		if(locale != null && locale.equals(Locale.CHINESE))
			return "Rotation日志文件";
		return "Rotation Log File";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "일정 주기마다 다른 경로에 백업 후 삭제하고 다시 쓰는 로그 파일을 수집합니다.";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "一定周期で他の経路にバックアップしたあと削除し、書き換えるログファイルを収集します。";
		if(locale != null && locale.equals(Locale.CHINESE))
			return "采集定期备份到其他路径之后删除并重新写的日志文件。";
		return "collect rotation text log file";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption filePath = new StringConfigType("file_path", t("File Path", "파일 경로", "ファイル経路","文件路径"), t("Log file path",
				"텍스트 로그 파일의 절대 경로", "テキストログファイルの絶対経路","文本日志文件的绝对路径"), true, t("/var/log/"));

		LoggerConfigOption datePattern = new StringConfigType("date_pattern", t("Date Pattern", "날짜 정규식", "日付正規表現", "日期正则表达式"), t(
				"Regex for date extraction", "날짜 문자열 추출에 사용되는 정규표현식", "日付文字列の抽出に使う正規表現", "用于提取日期字符串的正则表达式"), false, t(null));

		LoggerConfigOption dateFormat = new StringConfigType("date_format", t("Date Format", "날짜 패턴", "日付パターン", "日期模式"), t(
				"Date pattern of log file", "날짜 파싱에 필요한 패턴 (예시: yyyy-MM-dd HH:mm:ss)", "日付の解析に使うパターン (例: yyyy-MM-dd HH:mm:ss)", "用于解析日期的特征"),
				false, t("MMM dd HH:mm:ss"));

		LoggerConfigOption dateLocale = new StringConfigType("date_locale", t("Date Locale", "날짜 로케일", "日付ロケール", "日期区域"), t(
				"Date locale of log file", "날짜 문자열의 로케일. 가령 날짜 패턴의 MMM 지시어은 영문 로케일에서 Jan으로 인식됩니다.", "日付文字列ののロケール", "日期字符串区域"), false,
				t("en"));

		LoggerConfigOption charset = new StringConfigType("charset", t("Charset", "문자 집합", "文字セット", "字符集"), t("Charset",
				"문자 집합. 기본값 UTF-8", "文字セット。基本値はutf-8", "字符集，默认值为UTF-8"), false, t("utf-8"));

		LoggerConfigOption timezone = new StringConfigType("timezone", t("Time zone", "시간대","時間帯", "时区"), t("time zone, e.g. America/New_york ",
				"시간대, 예를 들면 KST 또는 Asia/Seoul","時間帯。例えばJSTまたはAsia/Tokyo", "时区， 例如 Asia/Beijing"), false);
		
		LoggerConfigOption logBeginRegex = new StringConfigType("begin_regex", t("Log begin regex", "로그 시작 구분 정규식", "ログ始め正規表現", "日志起始正则表达式"),
				t("Regular expression to determine whether the line is start of new log."
						+ "(if a line does not matches, the line will be merged to prev line.).",
						"새 로그의 시작을 인식하기 위한 정규식(매칭되지 않는 경우 이전 줄에 병합됨)", 
						"新しいログの始まりを認識する正規表現 (マッチングされない場合は前のラインに繋げる)",
						"用于识别日志起始位置的正则表达式(如没有匹配项，则合并到之前日志)"), false);

		LoggerConfigOption logEndRegex = new StringConfigType("end_regex", t("Log end regex", "로그 끝 구분 정규식", "ログ終わり正規表現", "日志结束正则表达式"), t(
				"Regular expression to determine whether the line is end of new log."
						+ "(if a line does not matches, the line will be merged to prev line.).",
				"로그의 끝을 인식하기 위한 정규식(매칭되지 않는 경우 이전 줄에 병합됨)", 
				"ログの終わりを認識する正規表現 (マッチングされない場合は前のラインに繋げる)",
				"用于识别日志结束位置地正则表达式(如没有匹配项，则合并到之前日志)"), false);

		return Arrays.asList(filePath, charset, datePattern, dateFormat, dateLocale, timezone, logBeginRegex, logEndRegex);
	}

	private Map<Locale, String> t(String text) {
		return t(text, text, text, text);
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
