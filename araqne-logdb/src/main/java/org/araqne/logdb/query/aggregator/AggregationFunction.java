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
package org.araqne.logdb.query.aggregator;

import java.util.List;

import org.araqne.logdb.Row;
import org.araqne.logdb.query.expr.Expression;

public interface AggregationFunction {
	String getName();

	List<Expression> getArguments();

	void apply(Row map);

	Object eval();

	void merge(AggregationFunction func);

	Object[] serialize();

	void deserialize(Object[] values);

	void clean();

	AggregationFunction clone();

	// True if
	// AggragtionFunction of Table
	// has equivalent meaning with
	// Reducer of (Mapper of SubTables)
	// Ex)
	// Count of Table
	// is equivalent with
	// Sum of (Count of SubTables)
	boolean canBeDistributed();

	AggregationFunction mapper(List<Expression> exprs);

	AggregationFunction reducer(List<Expression> exprs);
}
