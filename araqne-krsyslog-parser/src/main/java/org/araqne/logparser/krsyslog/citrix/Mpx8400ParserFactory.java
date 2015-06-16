package org.araqne.logparser.krsyslog.citrix;

import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

@Component(name = "mpx8400-parser-factory")
@Provides
public class Mpx8400ParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "mpx8400";
	}

	@Override
	public String getDisplayName(Locale locale) {
		return "MPX 8400";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "MPX 8400의 로그를 파싱합니다.";
		return "Parse MPX 8400 logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		return new Mpx8400Parser();
	}

}
