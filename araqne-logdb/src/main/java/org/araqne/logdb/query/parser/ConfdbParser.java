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
import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
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
public class ConfdbParser extends AbstractQueryCommandParser {

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
		//	throw new QueryParseException("no-read-permission", -1, "admin only");
			throw new QueryParseException("10000", -1, -1, null);

		String s = commandString.substring(getCommandName().length());
		QueryTokens tokens = QueryTokenizer.tokenize(s);

		if (tokens.size() < 1)
		//	throw new QueryParseException("missing-confdb-op", -1);
			throw new QueryParseException("10001", getCommandName().length() + 1, commandString.length() - 1, null);
			
		
		Confdb.Op op = null;
		try {
			op = Confdb.Op.parse(tokens.string(0));;
		} catch (QueryParseException t) {
			if(t.getType().equals("10004")){
				String ops = t.getParams().get("op");
				int offset = QueryTokenizer.findKeyword(commandString,ops, getCommandName().length());
				throw new QueryParseException(t.getType(), offset,  offset + ops.length() - 1,  t.getParams() );
			}else{
				throw t;
			}
		}
		
		
		ConfdbOptions options = new ConfdbOptions();
		options.setOp(op);

		if (op == Op.COLS) {
			if (tokens.size() < 2)
			//	throw new QueryParseException("missing-confdb-dbname", -1);
				throw new QueryParseException("10002", commandString.length() - 1, commandString.length() - 1, null);
				options.setDbName(tokens.string(1));
		} else if (op == Op.DOCS) {
			if (tokens.size() < 3)
			//	throw new QueryParseException("missing-confdb-colname", -1);
				throw new QueryParseException("10003", commandString.length() - 1,  commandString.length() - 1, null);
			options.setDbName(tokens.string(1));
			options.setColName(tokens.string(2));
		}

		return new Confdb(conf, options);
	}

}
