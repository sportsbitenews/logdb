package org.araqne.logparser.krsyslog.secuwiz;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

@Component(name = "secuwiz-sslvpn-log-parser-factory")
@Provides
public class SecuwizSslLogParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "secuwiz-sslvpn";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "시큐위즈 SSLVPN";
		return "Secuwiz SSLVPN";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "시큐위즈 SSL VPN 장비의 시스로그를 파싱합니다.";
		return "Parse Secuwiz SSLVPN syslogs.";
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		return new SecuwizSslLogParser();
	}

}
