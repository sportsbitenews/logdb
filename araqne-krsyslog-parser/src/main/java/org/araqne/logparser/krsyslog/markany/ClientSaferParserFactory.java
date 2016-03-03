package org.araqne.logparser.krsyslog.markany;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

// 현재는 해당 파서를 사용할 일이 없으므로 metadata.xml에서 해당 팩토리 삭제
@Component(name = "markany-client-safer-parser-factory")
@Provides
public class ClientSaferParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "markany-client-safer";
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
			return "마크애니 ClientSAFER";
		return "Markany ClientSAFER";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "마크애니 ClientSAFER 로그를 파싱합니다.";
		return "Parse Markany ClientSAFER logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		return new ClientSaferParser();
	}
}
