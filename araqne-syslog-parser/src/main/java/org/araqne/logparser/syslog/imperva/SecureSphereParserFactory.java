package org.araqne.logparser.syslog.imperva;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

@Component(name = "secure-sphere-parser-factory")
@Provides
public class SecureSphereParserFactory extends AbstractLogParserFactory {
	@Override
	public String getName() {
		return "secure-sphere";
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
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINESE);
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.CHINESE);
	}

	@Override
	public String getDisplayName(Locale locale) {
		return "SecureSphere";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "SecureSphere의 로그를 파싱합니다.";
		if (locale != null && locale.equals(Locale.CHINESE))
			return "解析SecureSphere日志。";
		return "Parse SecureSphere logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> config) {
		return new SecureSphereParser();
	}
}
