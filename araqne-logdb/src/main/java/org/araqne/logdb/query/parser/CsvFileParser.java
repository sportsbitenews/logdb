package org.araqne.logdb.query.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.FilePathHelper;
import org.araqne.logdb.LocalFilePathHelper;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.CsvFile;

public class CsvFileParser extends AbstractQueryCommandParser {

	public CsvFileParser() {
		setDescriptions("Read CSV file.", "CSV 파일에서 데이터를 조회합니다.");
		setOptions("offset", OPTIONAL, "Skip input count", "건너뛸 레코드 갯수");
		setOptions("limit", OPTIONAL, "Max output count", "가져올 최대 레코드 갯수");
	}

	@Override
	public String getCommandName() {
		return "csvfile";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("10800", new QueryErrorMessage("invalid-csvfile-path", "[file]이 존재하지 않거나 읽을수 없습니다."));
		m.put("10801", new QueryErrorMessage("invalid-parentfile-path", "[file]의 상위 디렉토리가 존재하지 않거나 읽을 수 없습니다."));
		m.put("10802", new QueryErrorMessage("missing-field", "파일경로을 입력하십시오."));
		return m;
	}

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), new ArrayList<String>(),
				getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;
		String filePath = commandString.substring(r.next).trim();
		filePath = ExpressionParser.evalContextReference(context, filePath, getFunctionRegistry());

		if (filePath.trim().isEmpty())
			throw new QueryParseException("10802", commandString.trim().length() - 1, commandString.trim().length() - 1, null);

		long offset = 0;
		if (options.containsKey("offset"))
			offset = Integer.valueOf(options.get("offset"));

		long limit = 0;
		if (options.containsKey("limit"))
			limit = Integer.valueOf(options.get("limit"));

		String cs = "utf-8";
		if (options.containsKey("cs"))
			cs = options.get("cs");

		try {
			FilePathHelper pathHelper = new LocalFilePathHelper(filePath);

			return new CsvFile(pathHelper.getMatchedFilePaths(), filePath, offset, limit, cs);
		} catch (IllegalStateException e) {
			String msg = e.getMessage();
			Map<String, String> params = new HashMap<String, String>();
			params.put("file", filePath);
			int offsetS = QueryTokenizer.findKeyword(commandString, filePath, getCommandName().length());
			String type = null;
			if (msg.equals("file-not-found"))
				type = "10800";
			else
				type = "10801";

			throw new QueryParseException(type, offsetS, offsetS + filePath.length() - 1, params);
		}
	}

}
