package org.araqne.logdb.cep.logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.araqne.log.api.AbstractLoggerFactory;
import org.araqne.log.api.Logger;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.MutableStringConfigType;
import org.araqne.logdb.cep.EventContextService;

@Component(name = "cep-event-logger-registry")
@Provides
public class CepEventLoggerFactory extends AbstractLoggerFactory {

	@Requires
	private EventContextService eventContextService;

	@Override
	public String getName() {
		return "cepevent";
	}

	@Override
	public List<Locale> getLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINESE, Locale.JAPANESE);
	}
	
	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "CEP 이벤트";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "CEP事件采集器";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "CEPイベント";

		return "CEP Event";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "CEP 컨텍스트 이벤트를 수집합니다.";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "采集CEP事件。";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "CEPコンテキストイベントを収取します。";
		return "Collect CEP context events";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption topics = new MutableStringConfigType("topics", t("Event Topics", "이벤트 주제 목록", "事件topic列表",
				"イベントトピックリスト"),
				t("Comma separated event topics", "쉼표로 구분된 이벤트 주제 목록", "输入事件topic列表(逗号分隔)。", "コンマ区切りのイベントトピックリスト"), true);
		return Arrays.asList(topics);
	}

	private Map<Locale, String> t(String en, String ko, String cn, String jp) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, en);
		m.put(Locale.KOREAN, ko);
		m.put(Locale.CHINESE, cn);
		m.put(Locale.JAPANESE, jp);
		return m;
	}

	@Override
	protected Logger createLogger(LoggerSpecification spec) {
		return new CepEventLogger(spec, this, eventContextService);
	}
}
