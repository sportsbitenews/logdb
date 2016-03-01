/*
 * Copyright 2013 Future Systems
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

import java.util.Arrays;

import org.araqne.logdb.Row;
import org.araqne.logdb.Strings;
import org.araqne.logdb.VectorizedRowBatch;

public class StringConstant implements VectorizedExpression {

	private String str;

	public StringConstant(String str) {
		this.str = str;
	}

	@Override
	public Object eval(Row map) {
		return str;
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		return str;
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] o = new Object[vbatch.size];
		Arrays.fill(o, str);
		return o;
	}

	public String getConstant() {
		return str;
	}

	@Override
	public String toString() {
		return Strings.doubleQuote(str);
	}
}
