package org.araqne.logparser.syslog.forescout;

import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

@Component(name = "counteract-log-parser-factory")
@Provides
public class CounterActLogParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "counteract";
	}

	@Override
	public String getDisplayName(Locale locale) {
		return "CounterACT NAC";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "ForeScout CounterACT NAC 로그를 파싱합니다.";
		return "Parse ForeScout CounterACT NAC logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		return new CounterActLogParser();
	}

}
