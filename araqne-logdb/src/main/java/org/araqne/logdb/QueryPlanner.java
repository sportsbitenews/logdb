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
package org.araqne.logdb;

import java.util.List;

public interface QueryPlanner {
	/**
	 * @since 2.1.7
	 * @return planner name
	 */
	String getName();

	boolean isEnabled();

	void setEnabled(boolean b);

	/**
	 * planner may throw parse exception
	 * 
	 * @return the new query execution pipeline
	 */
	List<QueryCommand> plan(QueryContext context, List<QueryCommand> commands);
}
