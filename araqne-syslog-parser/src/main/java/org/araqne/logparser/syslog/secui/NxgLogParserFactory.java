package org.araqne.logparser.syslog.secui;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserFactory;
import org.araqne.log.api.LoggerConfigOption;

@Component(name = "nxg-log-parser-factory")
@Provides
public class NxgLogParserFactory implements LogParserFactory {

	@Override
	public String getName() {
		return "nxg";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH);
	}

	@Override
	public String getDisplayName(Locale locale) {
		return "NXG";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH);
	}

	@Override
	public String getDescription(Locale locale) {
		return "SECUI NXG";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		return new ArrayList<LoggerConfigOption>();
	}

	@Override
	public LogParser createParser(Map<String, String> config) {
		return new NxgLogParser();
	}

}
