package org.araqne.log.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.araqne.cron.MinutelyJob;

@MinutelyJob
@Component(name = "time-rolling-log-writer-factory")
@Provides
public class TimeRollingLogWriterFactory extends AbstractLoggerFactory implements Runnable {
	@Requires
	private LoggerRegistry loggerRegistry;

	private ConcurrentMap<String, TimeRollingLogWriter> writers = new ConcurrentHashMap<String, TimeRollingLogWriter>();

	@Override
	public String getName() {
		return "totimefile";
	}

	@Override
	public List<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "시간별 롤링 로그 파일";
		return "time rolling logfile";
	}

	@Override
	public List<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "실시간으로 시간대별 롤링 로그 파일을 생성합니다.";
		return "write time rolling log file";
	}

	@Override
	public List<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption loggerName = new StringConfigType("source_logger", t("Source logger name", "원본 로거 이름"), t(
				"Full name of data source logger", "네임스페이스를 포함한 원본 로거 이름"), true);

		LoggerConfigOption filePath = new StringConfigType("file_path", t("file path", "파일 경로"), t("rolling file path", "롤링되는 파일 경로"), true);
		LoggerConfigOption rotateInterval = new StringConfigType("rotate_interval", t("rotate interval", "파일 교체 주기"), t("hour or day",
				"시간 (hour) 혹은 일자 (day)"), true);
		LoggerConfigOption charsetName = new StringConfigType("charset", t("charset", "문자집합"), t("utf-8 by default", "기본값은 utf-8"), false);

		return Arrays.asList(loggerName, filePath, rotateInterval, charsetName);
	}

	private Map<Locale, String> t(String enText, String koText) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, enText);
		m.put(Locale.KOREAN, koText);
		return m;
	}

	@Override
	public void run() {
		// force flush at least per 1min
		for (TimeRollingLogWriter writer : new ArrayList<TimeRollingLogWriter>(writers.values())) {
			writer.flush();
		}
	}

	@Override
	protected Logger createLogger(LoggerSpecification spec) {
		String fullName = spec.getNamespace() + "\\" + spec.getName();
		TimeRollingLogWriter writer = new TimeRollingLogWriter(spec, this, loggerRegistry);
		TimeRollingLogWriter old = writers.putIfAbsent(fullName, writer);
		if (old != null)
			throw new IllegalStateException("duplicated time rolling log writer: " + fullName);
		return writer;
	}

	@Override
	public void deleteLogger(String namespace, String name) {
		String fullName = namespace + "\\" + name;
		writers.remove(fullName);
		super.deleteLogger(namespace, name);
	}
}
