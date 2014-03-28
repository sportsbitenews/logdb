/**
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
package org.araqne.logdb.mongo.query;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandParser;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.mongo.MongoProfile;
import org.araqne.logdb.mongo.MongoProfileRegistry;
import org.araqne.logdb.query.parser.QueryTokenizer;
import org.araqne.logdb.query.parser.QueryTokens;

@Component(name = "mongo-query-parser")
public class MongoCommandParser implements QueryCommandParser {
	@Requires
	private QueryParserService queryParserService;

	@Requires
	private MongoProfileRegistry profileRegistry;

	@Override
	public String getCommandName() {
		return "mongo";
	}

	@Validate
	public void start() {
		queryParserService.addCommandParser(this);
	}

	@Invalidate
	public void stop() {
		if (queryParserService != null)
			queryParserService.removeCommandParser(this);
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		String s = commandString.substring(getCommandName().length());
		QueryTokens tokens = QueryTokenizer.tokenize(s);

		if (tokens.size() < 1)
			throw new QueryParseException("missing-mongo-profile", -1);

		if (tokens.size() < 2)
			throw new QueryParseException("missing-mongo-op", -1);

		MongoProfile profile = profileRegistry.getProfile(tokens.string(0));
		if (profile == null)
			throw new QueryParseException("invalid-mongo-profile", -1, tokens.string(0));

		MongoOp op = MongoOp.parse(tokens.string(1));
		if (op == null)
			throw new QueryParseException("invalid-mongo-op", -1, tokens.string(1));

		String col = tokens.string(2);

		String db = profile.getDefaultDatabase();
		int p = col.indexOf(".");
		if (p > 0) {
			db = col.substring(0, p);
			col = col.substring(p + 1);
		}

		if (db == null)
			throw new QueryParseException("missing-mongo-db", -1);

		// support find only
		MongoCommandOptions options = new MongoCommandOptions();
		options.setOp(op);
		options.setProfile(profile);
		options.setDatabase(db);
		options.setCollection(col);

		return new MongoFindCommand(options);
	}
}
