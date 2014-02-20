/*
 * Copyright 2014 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb.query.parser;

import org.araqne.confdb.ConfigService;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandParser;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.Confdb;
import org.araqne.logdb.query.command.Confdb.ConfdbOptions;
import org.araqne.logdb.query.command.Confdb.Op;

/**
 * @since 2.1.2
 * @author xeraph
 * 
 */
public class ConfdbParser implements QueryCommandParser {

	private ConfigService conf;

	public ConfdbParser(ConfigService conf) {
		this.conf = conf;
	}

	@Override
	public String getCommandName() {
		return "confdb";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		boolean allowed = context != null && context.getSession() != null && context.getSession().isAdmin();
		if (!allowed)
			throw new QueryParseException("no-read-permission", -1, "admin only");

		String s = commandString.substring(getCommandName().length());
		QueryTokens tokens = QueryTokenizer.tokenize(s);

		if (tokens.size() < 1)
			throw new QueryParseException("missing-confdb-op", -1);

		Confdb.Op op = Confdb.Op.parse(tokens.string(0));
		ConfdbOptions options = new ConfdbOptions();
		options.setOp(op);

		if (op == Op.COLS) {
			if (tokens.size() < 2)
				throw new QueryParseException("missing-confdb-dbname", -1);
			options.setDbName(tokens.string(1));
		} else if (op == Op.DOCS) {
			if (tokens.size() < 3)
				throw new QueryParseException("missing-confdb-colname", -1);
			options.setDbName(tokens.string(1));
			options.setColName(tokens.string(2));
		}

		return new Confdb(conf, options);
	}

}
