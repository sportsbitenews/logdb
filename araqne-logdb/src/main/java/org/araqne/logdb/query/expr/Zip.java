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
package org.araqne.logdb.query.expr;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.Strings;

/**
 * @since 2.4.30
 * @author kyun
 */
public class Zip implements Expression {

	private final int exprCount;
	private final List<Expression> exprs;

	public Zip(QueryContext ctx, List<Expression> exprs) {
		this.exprs = exprs;
		this.exprCount = exprs.size();
	}

	@Override
	public Object eval(Row row) {
		Iterator<?>[] iters = new Iterator<?>[exprCount];
		List<Object> lists = new ArrayList<Object>();

		int i = 0;
		for (Expression expr : exprs){
			Object list = expr.eval(row);
			lists.add(list);
			if (list instanceof List) 
				iters[i] = ((List<?>) list).iterator();
				i++;
		}

		BitSet used = new BitSet(exprCount);

		boolean empty = true;
		List<Object> zipped = new ArrayList<Object>();

		while (true) {
			List<Object> tuple = new ArrayList<Object>();

			boolean isNull = true;
			boolean eol = true;
			i = 0;
			for (Iterator<?> it : iters) 
			{
				Object value = null;

				if (it == null) {
					if (!used.get(i)) {
						value = lists.get(i);
						used.set(i);
						eol = false;
					}
				} else {
					if (it.hasNext()) {
						value = it.next();
						eol = false;
					}
				}

				if (value != null)
					isNull = false;

				tuple.add(value);
			
				i++;
				
			}
		
			if(eol)
				break;

			if (isNull)
				zipped.add(null);
			else {
				zipped.add(tuple);
				empty = false;
			}
		}
		return empty ? null : zipped;
	}
	
	@Override
	public String toString() {
		return "zip(" + Strings.join(exprs, ", ") + ")";
	}

}
