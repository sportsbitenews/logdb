/*
 * Copyright 2013 Eediom
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

import org.araqne.logdb.LogMap;

public class Comma implements Expression {
	private List<Expression> data = null;
	// if closed is true, new element cannot be added to end
	// for example, "(a, b), c" is a list has closed list "(a, b)" and c 
	private boolean closed = false;
	
	public boolean isClosed() {
		return closed;
	}
	
	public List<Expression> getList() {
		return data;
	}
	
	public Comma(Expression lhs, Expression rhs) {
		// lhs is and opened list or an expression
		if (lhs instanceof Comma) {
			Comma left = (Comma) lhs;
			if (!left.isClosed()) {
				data = left.data;
			}
		}		
		if (data == null) {
			data = new ArrayList<Expression>();
			data.add(lhs);
		}
		
		data.add(rhs);
	}
	
	public Comma(Expression lhs, Expression rhs, boolean closed) {
		this(lhs, rhs);
		this.closed = closed;
	}

	@Override
	public Object eval(LogMap map) {
		List<Object> ret = new ArrayList<Object>();
		for (Expression e:data) {
			ret.add(e.eval(map));
		}
		return ret;
	}

	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer(data.size()*5); // 5 is magic number
		buf.append("(");
		buf.append(data);
		buf.append(")");
		return buf.toString();
	}
}