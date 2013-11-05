/*
 * Copyright 2013 Eediom Inc.
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
package org.araqne.logdb.query.command;

import java.util.Map;

import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.query.expr.Expression;

public class Evalc extends LogQueryCommand {
	private final Map<String, Object> constants;
	private final String constantName;
	private final Expression expr;

	public Evalc(LogQueryContext context, String constantName, Expression expr) {
		this.constants = context.getConstants();
		this.constantName = constantName;
		this.expr = expr;
	}

	public String getConstantName() {
		return constantName;
	}

	public Expression getExpression() {
		return expr;
	}

	@Override
	public void push(LogMap m) {
		constants.put(constantName, expr.eval(m));
		write(m);
	}

	@Override
	public String toString() {
		return "evalc " + constantName + "=" + expr;
	}

}