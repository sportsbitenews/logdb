package org.araqne.logparser.krsyslog.jiran;

import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

@Component(name = "spamsniper-parser-factory")
@Provides
public class SpamSniperParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "spamsniper";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "스팸스나이퍼";
		return "SpamSniper";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "지란지교 스팸스나이퍼의 로그를 파싱합니다.";
		return "Parse SpamSniper logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		return new SpamSniperParser();
	}

}
