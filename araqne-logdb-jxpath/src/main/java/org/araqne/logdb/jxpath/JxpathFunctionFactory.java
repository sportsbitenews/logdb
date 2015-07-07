/**
 * Copyright 2015 Eediom Inc.
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
package org.araqne.logdb.jxpath;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.FunctionFactory;
import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.expr.Expression;

@Component(name = "jxpath-func-factory")
public class JxpathFunctionFactory implements FunctionFactory {

	@Requires
	private FunctionRegistry functionRegistry;

	@Validate
	public void start() {
		functionRegistry.registerFactory(this);
	}

	@Invalidate
	public void stop() {
		if (functionRegistry != null)
			functionRegistry.unregisterFactory(this);
	}

	@Override
	public Set<String> getFunctionNames() {
		return new HashSet<String>(Arrays.asList("jxpath"));
	}

	@Override
	public Expression newFunction(QueryContext ctx, String name, List<Expression> exprs) {
		return new JxpathFunction(ctx, exprs);
	}
}
