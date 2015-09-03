package org.araqne.logparser.krsyslog.citrix;

import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

@Component(name = "netscaler-mpx-parser-factory")
@Provides
public class NetScalerMpxParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "netscaler-mpx";
	}

	@Override
	public String getDisplayName(Locale locale) {
		return "Citrix NetScaler MPX";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "Citrix NetScaler MPX 로그를 파싱합니다.";
		return "Parse Citrix NetScaler MPX logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		return new NetScalerMpxParser();
	}

}
