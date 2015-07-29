package org.araqne.log.api;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;

@Component(name = "config-watch-logger-factory")
@Provides
public class ConfigWatchLoggerFactory extends AbstractLoggerFactory {

	@Override
	public String getName() {
		return "config-watch";
	}

	@Override
	public List<Locale> getLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINESE, Locale.JAPANESE);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "설정 파일 변경 탐지";
		if (locale.equals(Locale.CHINESE))
			return "配置文件监控";
		if (locale.equals(Locale.JAPANESE))
			return "設定ファイル変更探知";
		return "Config File Watcher";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "설정 파일이 변경될 때마다 로그를 발생합니다.";
		if (locale.equals(Locale.CHINESE))
			return "检测配置文件变化，并采集日志。";
		if (locale.equals(Locale.JAPANESE))
			return "設定ファイルが変更される時に、ログが発生します。";
		return "Detect config file changes and write logs.";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption basePath = new MutableStringConfigType("base_path", t("Directory path", "디렉터리 경로", "ディレクトリ経路", "目录"),
				t("Base log file directory path", "로그 파일을 수집할 대상 디렉터리 경로", "ログファイルを収集する対象ディレクトリ経路", "要采集的日志文件所在目录"), true, Subtype.LocalDirectory);

		LoggerConfigOption fileNamePattern = new MutableStringConfigType("filename_pattern", t("Filename pattern", "파일이름 패턴",
				"ファイルなパータン", "文件名模式"), t("Regular expression to match log file name", "대상 로그 파일을 선택하는데 사용할 정규표현식",
				"対象ログファイルを選ぶとき使う正規表現", "用于筛选文件的正则表达式"), true, Subtype.Regex);

		return Arrays.asList(basePath, fileNamePattern);
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
		return new ConfigWatchLogger(spec, this);
	}

}