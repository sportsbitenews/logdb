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

import org.araqne.logdb.mongo.MongoProfile;

public class MongoCommandOptions {

	private MongoProfile profile;
	private MongoOp op;

	private String database;
	private String collection;

	public MongoProfile getProfile() {
		return profile;
	}

	public void setProfile(MongoProfile profile) {
		this.profile = profile;
	}

	public MongoOp getOp() {
		return op;
	}

	public void setOp(MongoOp op) {
		this.op = op;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getCollection() {
		return collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	@Override
	public String toString() {
		return "profile=" + profile + ", op=" + op + ", database=" + database + ", collection=" + collection;
	}
}
