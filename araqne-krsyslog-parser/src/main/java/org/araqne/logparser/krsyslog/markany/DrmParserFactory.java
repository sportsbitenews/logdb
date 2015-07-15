package org.araqne.logparser.krsyslog.markany;

import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

@Component(name = "drm-parser-factory")
@Provides
public class DrmParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "drm";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "마크애니 DRM";
		return "Markany DRM";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "마크애니 DRM 로그를 파싱합니다.";
		return "Parse Markany DRM logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		return new DrmParser();
	}

}

