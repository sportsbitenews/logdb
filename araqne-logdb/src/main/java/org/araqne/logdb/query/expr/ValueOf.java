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
package org.araqne.logdb.query.expr;

import java.util.List;
import java.util.Map;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;

public class ValueOf implements Expression {

	private Expression compound;
	private Expression key;

	public ValueOf(List<Expression> args) {
		if (args.size() < 2)
			throw new QueryParseException("insufficient-valueof-args", -1);

		this.compound = args.get(0);
		this.key = args.get(1);
	}

	@Override
	public Object eval(Row row) {
		Object c = compound.eval(row);
		Object k = key.eval(row);

		if (c == null || k == null)
			return null;

		if (c instanceof Map) {
			return ((Map<?, ?>) c).get(k);
		} else if (c instanceof List && k instanceof Integer) {
			return ((List<?>) c).get((Integer) k);
		}

		return null;
	}

}
