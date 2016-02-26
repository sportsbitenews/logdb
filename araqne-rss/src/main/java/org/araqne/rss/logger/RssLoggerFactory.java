package org.araqne.rss.logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
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
import org.araqne.rss.RssReader;

@Component(name = "araqne-rss-logger-factory")
@Provides
public class RssLoggerFactory extends AbstractLoggerFactory {

	@Requires
	private RssReader rssReader;

	@Override
	public String getName() {
		return "rss";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "RSS 수집기";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "RSS収集機";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "RSS采集器";

		return "RSS";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "RSS 통해 로그를 수신합니다.";
		else
			return "Collect RSS log";
	}

	@Override
	public String getDisplayGroup(Locale locale) {
		return "RSS";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption basePath = new MutableStringConfigType("rss", t("RSS URL", "RSS 주소"),
				t("RSS URL", "로그를 수집할 대상 RSS 주소"), true);

		LoggerConfigOption stripTag = new MutableStringConfigType("strip", t("Strip tag", "태그 제거 여부"), t("Strip tag", "태그 제거 여부"),
				false);

		return Arrays.asList(basePath, stripTag);
	}

	private Map<Locale, String> t(String enText, String koText) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, enText);
		m.put(Locale.KOREAN, koText);
		return m;
	}

	@Override
	protected Logger createLogger(LoggerSpecification spec) {
		return new RssLogger(spec, this, rssReader);
	}

}
