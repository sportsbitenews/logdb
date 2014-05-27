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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.mongo.MongoProfile;
import org.bson.types.BSONTimestamp;
import org.bson.types.BasicBSONList;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MongoFindCommand extends DriverQueryCommand {
	private final Logger slog = LoggerFactory.getLogger(MongoFindCommand.class);

	private MongoCommandOptions options;

	public MongoFindCommand(MongoCommandOptions options) {
		this.options = options;
	}

	@Override
	public String getName() {
		return "mongo";
	}

	@Override
	public void run() {
		MongoProfile profile = options.getProfile();

		MongoClient mongo = null;
		DBCursor cursor = null;
		try {
			mongo = new MongoClient(profile.getAddress(), profile.getCredentials());

			DB db = mongo.getDB(options.getDatabase());
			DBCollection col = db.getCollection(options.getCollection());
			cursor = col.find();

			while (cursor.hasNext()) {
				DBObject doc = cursor.next();

				Map<String, Object> m = convert(doc);
				pushPipe(new Row(m));
			}
		} catch (Throwable t) {
			slog.error("araqne logdb mongo: cannot run mongo.find", t);
		} finally {
			if (cursor != null)
				cursor.close();

			if (mongo != null)
				mongo.close();
		}
	}

	private Map<String, Object> convert(DBObject doc) {
		Map<String, Object> m = new HashMap<String, Object>();

		for (String key : doc.keySet()) {
			m.put(key, convert(doc.get(key)));
		}

		return m;
	}

	/**
	 * convert bson types to java primitives. BasicBSONList, Binary,
	 * BSONTimestamp, Code, CodeWScope, MinKey, MaxKey, Symbol, ObjectId
	 */
	private Object convert(Object o) {
		if (o instanceof BSONTimestamp) {
			return ((BSONTimestamp) o).getTime();
		} else if (o instanceof Symbol || o instanceof Code || o instanceof CodeWScope || o instanceof MinKey
				|| o instanceof MaxKey || o instanceof ObjectId) {
			return o.toString();
		} else if (o instanceof BasicBSONList) {
			List<Object> l = new ArrayList<Object>();
			for (Object item : ((BasicBSONList) o)) {
				l.add(convert(item));
			}

			return l;
		} else {
			return o;
		}
	}
}
