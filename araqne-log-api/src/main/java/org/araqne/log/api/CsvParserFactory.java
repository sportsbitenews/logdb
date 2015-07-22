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

@Component(name = "csv-log-parser-factory")
@Provides
public class CsvParserFactory extends AbstractLogParserFactory {

	@Override
	public String getName() {
		return "csv";
	}

	@Override
	public String getDisplayName(Locale locale) {
		return "CSV";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "CSV 형식으로 구분된 각 토큰에 대하여 설정된 필드 이름들을 순서대로 적용하여 파싱합니다.";
		return "Divide a string into tokens based on the csv format and column names.";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		List<LoggerConfigOption> options = new ArrayList<LoggerConfigOption>();

		options.add(new StringConfigType("use_tab", t("Use tab", "탭문자 사용 여부"), t(
				"Use tab to delimiter. (true or false)",
				"구분자로 탭문자를 사용할지 결정합니다. true 혹은 false"), false));

		options.add(new StringConfigType("use_double_quote", t("Use double quote", "큰 따옴표 사용 여부"), t(
				"Use double quote to escape. (true or false)",
				"특수문자를 구분하기 위한 이스케이프 문자를 큰 따옴표로 사용합니다. true 혹은 false"), false));

		options.add(new StringConfigType("column_headers", t("Column headers", "필드 이름 목록"),
				t("Column headers", "파싱된 결과 필드 이름들"), false));

		options.add(new StringConfigType("target_field", t("Target field", "대상 필드"),
				t("Target field name", "파싱할 대상 필드 이름"), false));

		options.add(new StringConfigType("include_target", t("Include target", "원본 값 포함 여부"),
				t("Return also target field (true or false)", "CSV로 파싱된 결과 외에 원본 필드 값도 포함할지 설정합니다. true 혹은 false"), false));

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
		boolean useTab = Boolean.parseBoolean(configs.get("use_tab"));
		boolean useDoubleQuote = Boolean.parseBoolean(configs.get("use_double_quote"));

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
		return new CsvLogParser(useTab, useDoubleQuote, columnHeaders, targetField, includeTargetField);
	}

}
