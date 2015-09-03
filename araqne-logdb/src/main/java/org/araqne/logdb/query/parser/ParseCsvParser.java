package org.araqne.logdb.query.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandHelp;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.command.ParseCsv;

public class ParseCsvParser extends AbstractQueryCommandParser {

	public ParseCsvParser() {
		setDescriptions("Divide a string into tokens based on the csv format and column names.",
				"CSV 형식으로 구분된 각 토큰에 대하여 설정된 필드 이름들을 순서대로 적용하여 파싱합니다.");

		setOptions("field", OPTIONAL, "Target field name", "파싱할 대상 필드 이름");
		setOptions("overlay", OPTIONAL, "Return also original field (t or f)", "CSV로 파싱된 결과 외에 원본 필드 값도 포함할지 설정합니다. t 혹은 f");
		setOptions("tab", OPTIONAL, "Use tab to delimiter. (t or f)", "CSV로 파싱된 결과 외에 원본 필드 값도 포함할지 설정합니다. t 혹은 f");

		setUsages("parsecsv [overlay=t] [tab=t] [field=TARGET_FIELD] [FIELD_NAME1, FIELD_NAME2, ...]",
				"parsecsv [overlay=t] [tab=t] [field=대상필드] [필드이름1, 필드이름2, ...]");
	}

	@Override
	public String getCommandName() {
		return "parsecsv";
	}

	@Override
	public QueryCommandHelp getCommandHelp() {
		return help;
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("field", "overlay", "tab"), getFunctionRegistry());

		Map<String, String> options = (Map<String, String>) r.value;
		String field = options.get("field");
		if (field == null)
			field = "line";

		boolean overlay = CommandOptions.parseBoolean(options.get("overlay"));
		boolean useTab = CommandOptions.parseBoolean(options.get("tab"));

		String nameField = commandString.substring(r.next).trim();
		List<String> fieldNames = null;
		if (!nameField.isEmpty()) {
			fieldNames = new ArrayList<String>();
			for (String fieldName : nameField.split(",")) {
				fieldNames.add(fieldName.trim());
			}
		}
		return new ParseCsv(field, overlay, useTab, fieldNames);
	}
}
