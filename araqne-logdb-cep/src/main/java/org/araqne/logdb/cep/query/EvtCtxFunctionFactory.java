package org.araqne.logdb.cep.query;

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
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.query.expr.Expression;

@Component(name = "cep-function-factory")
public class EvtCtxFunctionFactory implements FunctionFactory {

	@Requires
	private FunctionRegistry functionRegistry;

	@Requires
	private EventContextService eventContextService;

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
		return new HashSet<String>(Arrays.asList("evtctxget", "evtctxgetvar", "evtctxsetvar", "evtctxcnt"));
	}

	@Override
	public Expression newFunction(QueryContext ctx, String name, List<Expression> exprs) {
		if (name.equals("evtctxget")) {
			return new EvtCtxGetFunction(ctx, exprs, eventContextService);
		} else if (name.equals("evtctxgetvar")) {
			return new EvtCtxGetVarFunction(ctx, exprs, eventContextService);
		} else if (name.equals("evtctxsetvar")) {
			return new EvtCtxSetVarFunction(ctx, exprs, eventContextService);
		} else if (name.equals("evtctxcnt")) {
			return new EvtCtxCntFunction(ctx, exprs, eventContextService);
		}

		throw new QueryParseException("unsupported-function", -1, name);
	}

}
