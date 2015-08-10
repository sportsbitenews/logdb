package org.araqne.logparser.krsyslog.monitorlab;

import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

@Component(name = "wisg-parser-factory")
@Provides
public class WisgParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "wisg";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "모니터랩 WISG";
		return "Monitorapp WISG";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "모니터랩 WISG의 로그를 파싱합니다.";
		return "Parse Monitorapp WISG logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		return new WisgParser();
	}

}
