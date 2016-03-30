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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Strings;

public abstract class FunctionExpression implements VectorizedExpression {
	private final String name;
	private final Collection<Expression> args;
	
	public FunctionExpression(String name, Collection<Expression> args) {
		this.name = name;
		this.args = args;
	}
	
	public FunctionExpression(String name, Collection<Expression> args, int minArgCnt) {
		this.name = name;
		this.args = args;
		
		if (args.size() < minArgCnt){
			//throw new QueryParseException(name + "-arg-missing", -1);
			Map<String, String> params = new HashMap<String, String> ();
			params.put("name", name);
			params.put("min", minArgCnt +"");
			params.put("args", args.size() +"");
			throw new QueryParseException("99000", -1, -1, params);
		}
	}
	
	@Override
	public String toString() {
		return name + "(" + Strings.join(args, ", ") + ")";
	}

}
