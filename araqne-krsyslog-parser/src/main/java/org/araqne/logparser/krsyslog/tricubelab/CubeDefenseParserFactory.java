package org.araqne.logparser.krsyslog.tricubelab;

import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

@Component(name = "cube-defense-parser-factory")
@Provides
public class CubeDefenseParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "cube-defense";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "큐브 디펜스";
		return "CubeDefense";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "큐브 디펜스 장비의 로그를 파싱합니다.";
		return "Parse CubeDefense logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		return new CubeDefenseParser();
	}

}
