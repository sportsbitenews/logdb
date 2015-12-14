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

import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.Account;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.DefaultQuery;
import org.araqne.logdb.FunctionFactory;
import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.Procedure;
import org.araqne.logdb.ProcedureParameter;
import org.araqne.logdb.ProcedureRegistry;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.Row;
import org.araqne.logdb.Session;
import org.araqne.logdb.StreamResultFactory;
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
		setDescriptions(
				"Execute procedure. Procedure arguments are used as query parameter. This command requires procedure owner or granted permission.",
				"사용자 정의 프로시저를 실행합니다. 프로시저 매개변수 형식에 맞추어 인자를 넘겨주면, 인자가 쿼리 매개변수로 설정된 후 미리 정의된 쿼리가 실행됩니다. 프로시저의 소유자 혹은 권한을 부여받은 사용자가 프로시저의 소유자 권한으로 쿼리를 실행합니다.");
	}

	@Override
	public String getCommandName() {
		return "proc";
	}

	@Override
	public Map<String, QueryErrorMessage> getErrorMessages() {
		Map<String, QueryErrorMessage> m = new HashMap<String, QueryErrorMessage>();
		m.put("11000", new QueryErrorMessage("procedure-not-found", "프로시저를 찾을 수 없습니다."));
		m.put("11001", new QueryErrorMessage("procedure-variable-type-mismatch [type]",
				"프로시저 변수가 타입이 맞지 않습니다. [param]는 [type] 타입이여야 합니다."));
		m.put("11002", new QueryErrorMessage("procedure-owner-not-found", "프로시저 소유자를 찾을 수 없습니다."));
		m.put("11003", new QueryErrorMessage("procedure-parameter-mismatch",
				"프로시저의 인자 수가 맞지 않습니다. [preset]개의 인자가 필요한데 [params]개의 인자가 입력 됐습니다."));
		m.put("11004", new QueryErrorMessage("procedure-is-not-granted", "프로시저 실행 권한이 없습니다."));
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

		if (!isGranted(context, procedure))
			throw new QueryParseException("11004", getCommandName().length() + 1);

		// parameter evaluation
		List<Object> params = new ArrayList<Object>();
		for (Expression e : procFunc.exprs)
			params.add(e.eval(null));

		// parameter validation
		int i = 0;
		for (ProcedureParameter var : procedure.getParameters()) {
			Object param = params.get(i++);
			verifyParameterType(commandString, procedure, var, param);
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
			param = convertDateAndTime(param, v);
			procParams.put(v.getKey(), param);
		}

		// Constructor must not allocate resource.
		// The onStart will allocate it.
		// Need to close session to keep this contract.
		Session session = null;
		QueryContext procCtx = null;
		List<QueryCommand> procCommands = null;
		try {
			session = accountService.newSession(procedure.getOwner());

			procCtx = new QueryContext(session, context);
			for (String key : procParams.keySet()) {
				Object value = procParams.get(key);
				Map<String, Object> constants = procCtx.getConstants();
				constants.put(key, value);
			}

			procCommands = parserService.parseCommands(procCtx, procedure.getQueryString());
		} finally {
			accountService.logout(session);
		}

		return new Proc(procedure, commandString, accountService, procCtx, procCommands);
	}

	private boolean isGranted(QueryContext context, Procedure p) {
		if (context.getSession() == null)
			return true;

		return procedureRegistry.isGranted(p.getName(), context.getSession().getLoginName());
	}

	private Object convertDateAndTime(Object param, ProcedureParameter v) {
		if (v.getType().equals("date")) {
			if (param instanceof String) {
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
				param = df.parse(param.toString(), new ParsePosition(0));
			} else if (param instanceof Date) {
				// truncate to 00:00:00
				Calendar c = Calendar.getInstance();
				c.setTime((Date) param);
				c.set(Calendar.HOUR_OF_DAY, 0);
				c.set(Calendar.MINUTE, 0);
				c.set(Calendar.SECOND, 0);
				c.set(Calendar.MILLISECOND, 0);
				param = c.getTime();
			}
		}

		if (v.getType().equals("datetime") && param instanceof String) {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			param = df.parse(param.toString(), new ParsePosition(0));
		}

		return param;
	}

	private void verifyParameterType(String commandString, Procedure procedure, ProcedureParameter var, Object param) {
		if (var.getType().equals("string") && param != null && !(param instanceof String)) {
			int offset = QueryTokenizer.findKeyword(commandString, procedure.getName()) + procedure.getName().length();
			throw new QueryParseException("11001", offset, commandString.length() - 1, errorParams("string", param));
		} else if (var.getType().equals("datetime") && param != null && !(param instanceof String) && !(param instanceof Date)) {
			int offset = QueryTokenizer.findKeyword(commandString, procedure.getName()) + procedure.getName().length();
			throw new QueryParseException("11001", offset, commandString.length() - 1, errorParams("datetime", param));
		} else if (var.getType().equals("int") && param != null && !(param instanceof Integer)) {
			int offset = QueryTokenizer.findKeyword(commandString, procedure.getName()) + procedure.getName().length();
			throw new QueryParseException("11001", offset, commandString.length() - 1, errorParams("int", param));
		} else if (var.getType().equals("double") && param != null && !(param instanceof Double)) {
			int offset = QueryTokenizer.findKeyword(commandString, procedure.getName()) + procedure.getName().length();
			throw new QueryParseException("11001", offset, commandString.length() - 1, errorParams("double", param));
		} else if (var.getType().equals("bool") && param != null && !(param instanceof Boolean)) {
			int offset = QueryTokenizer.findKeyword(commandString, procedure.getName()) + procedure.getName().length();
			throw new QueryParseException("11001", offset, commandString.length() - 1, errorParams("bool", param));
		}

		// check string in date format
		if (var.getType().equals("date") && param != null && param instanceof String) {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			try {
				df.parse(param.toString());
			} catch (ParseException e) {
				int offset = QueryTokenizer.findKeyword(commandString, procedure.getName()) + procedure.getName().length();
				throw new QueryParseException("11001", offset, commandString.length() - 1, errorParams("date", param));
			}
		}

		// check string in datetime format
		if (var.getType().equals("datetime") && param != null && param instanceof String) {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			try {
				df.parse(param.toString());
			} catch (ParseException e) {
				int offset = QueryTokenizer.findKeyword(commandString, procedure.getName()) + procedure.getName().length();
				throw new QueryParseException("11001", offset, commandString.length() - 1, errorParams("datetime", param));
			}
		}

		// throw new QueryParseException("procedure-variable-type-mismatch", -1,
		// param.toString());
	}

	private Map<String, String> errorParams(String type, Object value) {
		Map<String, String> m = new HashMap<String, String>();
		m.put("type", type);
		m.put("param", value.toString());
		return m;
	}

	private class ProcFunctionRegistry implements FunctionRegistry {

		private ProcFunctionRegistry() {
		}

		@Override
		public Set<String> getFunctionNames() {
			Set<String> set = new HashSet<String>(parserService.getFunctionRegistry().getFunctionNames());
			set.addAll(procedureRegistry.getProcedureNames());
			return set;
		}

		@Override
		public Expression newFunction(QueryContext ctx, String functionName, List<Expression> exprs) {
			// parser service or function registry can be null at unit test
			if (parserService != null) {
				FunctionRegistry funcRegistry = parserService.getFunctionRegistry();
				if (funcRegistry != null && funcRegistry.getFunctionNames().contains(functionName))
					return funcRegistry.newFunction(ctx, functionName, exprs);
			}

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
}
