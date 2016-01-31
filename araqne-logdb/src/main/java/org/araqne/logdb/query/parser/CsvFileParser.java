package org.araqne.logdb.query.parser;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
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

	@SuppressWarnings("unchecked")
	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(), new ArrayList<String>(),
				getFunctionRegistry());
		Map<String, String> options = (Map<String, String>) r.value;
		String filePath = commandString.substring(r.next).trim();
		filePath = ExpressionParser.evalContextReference(context, filePath, getFunctionRegistry());

		long offset = 0;
		if (options.containsKey("offset"))
			offset = Integer.valueOf(options.get("offset"));

		long limit = 0;
		if (options.containsKey("limit"))
			limit = Integer.valueOf(options.get("limit"));
		
		String cs = "utf-8";
		if (options.containsKey("cs"))
			cs = options.get("cs");

		File f = new File(filePath);
		if (!f.exists() || !f.canRead())
			throw new QueryParseException("csv-file-not-found", -1);

		return new CsvFile(filePath, offset, limit, cs);
	}

}
