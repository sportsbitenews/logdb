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
package org.araqne.logdb.query.parser;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.Account;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.FunctionFactory;
import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.Procedure;
import org.araqne.logdb.ProcedureParameter;
import org.araqne.logdb.ProcedureRegistry;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.Row;
import org.araqne.logdb.Session;
import org.araqne.logdb.query.command.Proc;
import org.araqne.logdb.query.expr.Expression;

public class ProcParser extends AbstractQueryCommandParser {

	private AccountService accountService;
	private QueryParserService parserService;
	private ProcedureRegistry procedureRegistry;

	public ProcParser(AccountService accountService, QueryParserService parserService, ProcedureRegistry procedureRegistry) {
		this.accountService = accountService;
		this.parserService = parserService;
		this.procedureRegistry = procedureRegistry;
	}

	@Override
	public String getCommandName() {
		return "proc";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("11000", new QueryErrorMessage("procedure-not-found", "프로시저를 찾을 수 없습니다."));
		m.put("11001", new QueryErrorMessage("procedure-variable-type-mismatch [Type]",
				"프로시저 변수가 타입이 맞지 않습니다. [param]는 [type] 타입이여야 합니다."));
		m.put("11002", new QueryErrorMessage("procedure-owner-not-found", "프로시저 소유자를 찾을 수 없습니다."));
		m.put("11003", new QueryErrorMessage("procedure-parameter-mismatch",
				"프로시저의 인자 수가 맞지 않습니다. [preset]개의 인자가 필요한데 [params]개의 인자가 입력 됐습니다."));
		return m;
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ProcFunction procFunc = (ProcFunction) ExpressionParser.parse(context, commandString.substring(4),
				new ProcFunctionRegistry());

		Procedure procedure = procedureRegistry.getProcedure(procFunc.procedureName);
		if (procedure == null)
			// throw new QueryParseException("procedure-not-found", -1);
			throw new QueryParseException("11000", getCommandName().length() + 1, commandString.length() - 1, null);

		// parameter evaluation
		List<Object> params = new ArrayList<Object>();
		for (Expression e : procFunc.exprs)
			params.add(e.eval(null));

		// parameter validation
		int i = 0;
		for (ProcedureParameter var : procedure.getParameters()) {
			Object param = params.get(i++);
			if (var.getType().equals("string") && param != null && !(param instanceof String)) {
				Map<String, String> parameter = new HashMap<String, String>();
				parameter.put("param", param.toString());
				parameter.put("type", "string");
				int offset = QueryTokenizer.findKeyword(commandString, procedure.getName()) + procedure.getName().length();
				throw new QueryParseException("11001", offset, commandString.length() - 1, parameter);
				// throw new
				// QueryParseException("procedure-variable-type-mismatch", -1,
				// param.toString());
			} else if (var.getType().equals("int") && param != null && !(param instanceof Integer)) {
				Map<String, String> parameter = new HashMap<String, String>();
				parameter.put("param", param.toString());
				parameter.put("type", "int");
				int offset = QueryTokenizer.findKeyword(commandString, procedure.getName()) + procedure.getName().length();
				throw new QueryParseException("11001", offset, commandString.length() - 1, parameter);
				// throw new
				// QueryParseException("procedure-variable-type-mismatch", -1,
				// param.toString());
			} else if (var.getType().equals("double") && param != null && !(param instanceof Double)) {
				Map<String, String> parameter = new HashMap<String, String>();
				parameter.put("param", param.toString());
				parameter.put("type", "double");
				int offset = QueryTokenizer.findKeyword(commandString, procedure.getName()) + procedure.getName().length();
				throw new QueryParseException("11001", offset, commandString.length() - 1, parameter);
				// throw new
				// QueryParseException("procedure-variable-type-mismatch", -1,
				// param.toString());
			} else if (var.getType().equals("bool") && param != null && !(param instanceof Boolean)) {
				Map<String, String> parameter = new HashMap<String, String>();
				parameter.put("param", param.toString());
				parameter.put("type", "bool");
				int offset = QueryTokenizer.findKeyword(commandString, procedure.getName()) + procedure.getName().length();
				throw new QueryParseException("11001", offset, commandString.length() - 1, parameter);
				// throw new
				// QueryParseException("procedure-variable-type-mismatch", -1,
				// param.toString());
			}
		}

		// owner delegation
		Account account = accountService.getAccount(procedure.getOwner());
		if (account == null) {
			// throw new QueryParseException("procedure-owner-not-found", -1);
			Map<String, String> parameter = new HashMap<String, String>();
			parameter.put("owner", procedure.getOwner());
			throw new QueryParseException("11002", -1, -1, parameter);
		}

		if (procedure.getParameters().size() != procFunc.exprs.size()) {
			// throw new QueryParseException("procedure-parameter-mismatch",
			// -1);
			Map<String, String> parameter = new HashMap<String, String>();
			parameter.put("params", procFunc.exprs.size() + "");
			parameter.put("preset", procedure.getParameters().size() + "");

			throw new QueryParseException("11003", getCommandName().length() + 1, commandString.length() - 1, parameter);
		}

		Map<String, Object> procParams = new HashMap<String, Object>();
		i = 0;
		for (Object param : params) {
			ProcedureParameter v = procedure.getParameters().get(i++);
			procParams.put(v.getKey(), param);
		}

		return new Proc(procedure, procParams, commandString, parserService, accountService);
	}

	private class ProcFunctionRegistry implements FunctionRegistry {

		private ProcFunctionRegistry() {
		}

		@Override
		public Set<String> getFunctionNames() {
			return procedureRegistry.getProcedureNames();
		}

		@Override
		public Expression newFunction(QueryContext ctx, String functionName, List<Expression> exprs) {
			return new ProcFunction(functionName, exprs);
		}

		@Override
		public void registerFactory(FunctionFactory factory) {
		}

		@Override
		public void unregisterFactory(FunctionFactory factory) {
		}
	}

	private class ProcFunction implements Expression {

		private String procedureName;
		private List<Expression> exprs;

		public ProcFunction(String procedureName, List<Expression> exprs) {
			this.procedureName = procedureName;
			this.exprs = exprs;
		}

		@Override
		public Object eval(Row row) {
			return null;
		}
	}

	private class DummySession implements Session {
		private String guid;
		private Account account;
		private Map<String, Object> props;

		public DummySession(Account account) {
			this.guid = UUID.randomUUID().toString();
			this.account = account;
			props = new ConcurrentHashMap<String, Object>();
		}

		@Override
		public String getGuid() {
			return guid;
		}

		@Override
		public String getLoginName() {
			return account.getLoginName();
		}

		@Override
		public Date getCreated() {
			return new Date();
		}

		@Override
		public boolean isAdmin() {
			return account.isAdmin();
		}

		@Override
		public Set<String> getPropertyKeys() {
			return props.keySet();
		}

		@Override
		public Object getProperty(String name) {
			return props.get(name);
		}

		@Override
		public void setProperty(String name, Object value) {
			props.put(name, value);
		}

		@Override
		public void unsetProperty(String name) {
			if (props.containsKey(name))
				props.remove(name);
		}
	}
}
