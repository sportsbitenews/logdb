package org.araqne.log.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;

@Component(name = "hourly-file-logger-factory")
@Provides
public class TimeBasedRollingFileLoggerFactory extends AbstractLoggerFactory {

	@Override
	public String getName() {
		return "hourly-file";
	}

	@Override
	public String getDisplayName(Locale locale) {
		return "Time-based rolling file Logger";
	}

	@Override
	public String getDescription(Locale locale) {
		return "Watch current file with given format.";
	}

	@Override
	protected Logger createLogger(LoggerSpecification spec) {
		return new TimeBasedRollingFileLogger(spec, this);
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH);
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		ArrayList<Locale> locales = new ArrayList<Locale>();
		locales.add(Locale.ENGLISH);
		return locales;
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		List<LoggerConfigOption> types = new ArrayList<LoggerConfigOption>();

		types.add(
				new StringConfigTypeBuilder(
						TimeBasedRollingFileLogger.optNameFilePathFormat,
						"File path format (ex: /path/%d{yyyy-MM-dd}/prefix_%d{HH}.tmp)",
						"Java Logback-RollingFileAppender-style format of filepath",
						true).get());
		types.add(
				new StringConfigTypeBuilder(
						TimeBasedRollingFileLogger.optNameLastLogPath,
						"Path for .lastlog",
						"dir to save recent running information.",
						true).get());
		types.add(
				new StringConfigTypeBuilder(
						TimeBasedRollingFileLogger.optNameFileDuration,
						"Duration of each File(optional, hours, default: 1)",
						"'current file' will be determined by time that is floored by this value.",
						false, "1").get());
		types.add(
				new StringConfigTypeBuilder(
						TimeBasedRollingFileLogger.optNameStartTime,
						"date to start logging (optional, yyyyMMdd HHmm, default: current)",
						"date to start logging",
						false, "").get());
		types.add(
				new IntegerConfigTypeBuilder(
						TimeBasedRollingFileLogger.optNameCloseWaitMillisec,
						"Close wait duration (optional, millisec, default: 3000)",
						"duration to wait for last file closed on the file-changing-edge.",
						false, "3000").get());

		return types;
	}

}
