package org.araqne.logparser.syslog.symantec;

import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

@Component(name = "protection-engine-log-parser-factory")
@Provides
public class ProtectionEngineLogParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "symantec-protection-engine";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "시만텍 프로텍션 엔진";
		return "Symantec Protection Engine";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "시만텍 프로텍션 엔진 로그를 파싱 합니다.";
		return "Parse Symantec Protection Engine logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		return new ProtectionEngineLogParser();
	}

}