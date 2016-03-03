package org.araqne.logparser.krsyslog.kornicglory;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

@Component(name = "tess-audit-parser-factory")
@Provides
public class TessAuditParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "tessaudit";
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
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "코닉글로리 TESS AUDIT";
		return "TESS AUDIT";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "코닉글로리 TESS AUDIT 로그를 파싱합니다.";
		return "Parse TESS ADUIT logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		return new TessAuditParser();
	}
}
