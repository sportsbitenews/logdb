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
package org.araqne.logdb.query.parser;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.command.Drop;

public class DropParser extends AbstractQueryCommandParser {

	public DropParser() {
		setDescriptions("Drop all input data. You can use this command to measure net query performance.",
				"들어오는 모든 입력을 버립니다. 일반적으로 스크립트와 같이 부수적인 효과가 있는 쿼리 커맨드를 실행하고 출력 결과는 버릴 때나, 배치 실행만 하고 쿼리 결과는 필요없을 때, 이전 커맨드의 쿼리 수행 시간만을 측정하려고 할 때 사용합니다.");
		setUsages("drop", "drop");
	}

	@Override
	public String getCommandName() {
		return "drop";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		return new Drop();
	}
}
