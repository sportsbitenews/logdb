package org.araqne.logparser.krsyslog.samsung;

import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLogParserFactory;
import org.araqne.log.api.LogParser;

/**
 * @author xeraph
 */
@Component(name = "exshield-csv-parser-factory")
@Provides
public class ExshieldCsvParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "exshield-csv";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "엑쉴드 CSV";
		return "eXshield CSV";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "삼성 엑쉴드 시스로그(CSV)를 파싱합니다.";
		return "Parse Samsung eXshield CSV logs.";
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		return new ExshieldCsvParser();
	}

}
