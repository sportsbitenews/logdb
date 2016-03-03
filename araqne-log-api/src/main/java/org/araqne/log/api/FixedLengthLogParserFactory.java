package org.araqne.log.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;

@Component(name = "fixed-length-log-parser-factory")
@Provides
public class FixedLengthLogParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "fixed-length";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "고정 길이 필드";

		return "Fixed Length Fields";
	}

	@Override
	public String getDisplayGroup(Locale locale) {
		if (locale == Locale.KOREAN)
			return "일반";
		else
			return "General";
	}

	@Override
	public List<Locale> getLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "설정된 필드 이름들에 대하여 입력한 고정길이로 로그를 파싱합니다.";
		return "Divide a string into tokens based on the fixed length and field names.";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		List<LoggerConfigOption> options = new ArrayList<LoggerConfigOption>();
		options.add(new StringConfigType("column_headers", t("Fields", "필드 이름 목록"),
				t("Comma separated field names", "쉼표로 구분된 필드 이름 목록"), true));

		options.add(new StringConfigType("column_length", t("Field lengths", "필드 길이 목록"),
				t("Comma separated field lengths", "필드 별 길이"), false));

		options.add(
				new StringConfigType("target_field", t("Target field", "대상 필드"), t("Target field name", "파싱할 대상 필드 이름"), false));

		options.add(new StringConfigType("include_target", t("Include target", "원본 값 포함 여부"),
				t("Return also target field (true or false)", "고정 길이로 파싱된 결과 외에 원본 필드 값도 포함할지 설정합니다. true 혹은 false"), false));

		return options;
	}

	private Map<Locale, String> t(String enText, String koText) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, enText);
		m.put(Locale.KOREAN, koText);
		return m;
	}

	@Override
	public LogParser createParser(Map<String, String> configs) {
		Integer[] fieldLength = null;
		String[] length = null;
		String l = configs.get("column_length");
		if (l != null) {
			length = l.split(",");
			fieldLength = new Integer[length.length];
			for (int i = 0; i < length.length; i++)
				fieldLength[i] = Integer.parseInt(length[i].trim());
		}

		String[] columnHeaders = null;
		String h = configs.get("column_headers");
		if (h != null) {
			columnHeaders = h.split(",");
			for (int i = 0; i < columnHeaders.length; i++)
				columnHeaders[i] = columnHeaders[i].trim();
		}

		boolean includeTargetField = false;
		if (configs.containsKey("include_target")) {
			String s = configs.get("include_target");
			includeTargetField = (s != null && Boolean.parseBoolean(s));
		}

		String targetField = configs.get("target_field");
		if (targetField == null)
			targetField = "line";
		return new FixedLengthLogParser(targetField, includeTargetField, fieldLength, columnHeaders);
	}

}
