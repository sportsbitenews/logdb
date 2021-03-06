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
package org.araqne.logdb.query.expr;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;

/**
 * @since 1.7.8
 * @author xeraph
 * 
 */
public class Seq implements Expression {
	private AtomicLong index = new AtomicLong(1);
	
	public Seq(QueryContext ctx, List<Expression> exprs) {
	}

	@Override
	public Object eval(Row map) {
		return index.getAndIncrement();
	}

	@Override
	public String toString() {
		return "seq()";
	}
}
