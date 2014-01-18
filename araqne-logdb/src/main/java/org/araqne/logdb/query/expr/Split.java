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

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.araqne.logdb.Row;

public class Split implements Expression {

	private Expression target;
	private final String delimiters;

	public Split(List<Expression> exprs) {
		this.target = exprs.get(0);
		this.delimiters = exprs.get(1).eval(null).toString();
	}

	@Override
	public Object eval(Row row) {
		Object o = target.eval(row);
		if (o == null)
			return null;

		StringTokenizer tok = new StringTokenizer(o.toString(), delimiters);
		ArrayList<String> tokens = new ArrayList<String>();
		while (tok.hasMoreElements()) {
			tokens.add(tok.nextToken());
		}

		return tokens;
	}
}
