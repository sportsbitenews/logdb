package org.araqne.logparser.syslog.forescout;

import java.util.Arrays;
import java.util.List;
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
	public String getDisplayGroup(Locale locale) {
		if (locale == Locale.KOREAN)
			return "네트워크 보안";
		else
			return "Network Security";
	}

	@Override
	public List<Locale> getLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINESE);
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
