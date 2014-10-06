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
package org.araqne.logdb.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.logdb.FunctionFactory;
import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.expr.Abs;
import org.araqne.logdb.query.expr.Array;
import org.araqne.logdb.query.expr.Case;
import org.araqne.logdb.query.expr.Ceil;
import org.araqne.logdb.query.expr.Concat;
import org.araqne.logdb.query.expr.Contains;
import org.araqne.logdb.query.expr.ContextReference;
import org.araqne.logdb.query.expr.DateAdd;
import org.araqne.logdb.query.expr.DateDiff;
import org.araqne.logdb.query.expr.DateTrunc;
import org.araqne.logdb.query.expr.Decode;
import org.araqne.logdb.query.expr.Decrypt;
import org.araqne.logdb.query.expr.Encrypt;
import org.araqne.logdb.query.expr.Epoch;
import org.araqne.logdb.query.expr.Expression;
import org.araqne.logdb.query.expr.Field;
import org.araqne.logdb.query.expr.Flatten;
import org.araqne.logdb.query.expr.Floor;
import org.araqne.logdb.query.expr.FromBase64;
import org.araqne.logdb.query.expr.Guid;
import org.araqne.logdb.query.expr.Hash;
import org.araqne.logdb.query.expr.If;
import org.araqne.logdb.query.expr.In;
import org.araqne.logdb.query.expr.IndexOf;
import org.araqne.logdb.query.expr.Ip2Long;
import org.araqne.logdb.query.expr.IsNotNull;
import org.araqne.logdb.query.expr.IsNull;
import org.araqne.logdb.query.expr.IsNum;
import org.araqne.logdb.query.expr.IsStr;
import org.araqne.logdb.query.expr.KvJoin;
import org.araqne.logdb.query.expr.Left;
import org.araqne.logdb.query.expr.Len;
import org.araqne.logdb.query.expr.Long2Ip;
import org.araqne.logdb.query.expr.Lower;
import org.araqne.logdb.query.expr.Match;
import org.araqne.logdb.query.expr.Max;
import org.araqne.logdb.query.expr.Min;
import org.araqne.logdb.query.expr.Network;
import org.araqne.logdb.query.expr.Now;
import org.araqne.logdb.query.expr.Rand;
import org.araqne.logdb.query.expr.RandBytes;
import org.araqne.logdb.query.expr.RexReplace;
import org.araqne.logdb.query.expr.Right;
import org.araqne.logdb.query.expr.Round;
import org.araqne.logdb.query.expr.Seq;
import org.araqne.logdb.query.expr.Split;
import org.araqne.logdb.query.expr.StrJoin;
import org.araqne.logdb.query.expr.StringReplace;
import org.araqne.logdb.query.expr.Substr;
import org.araqne.logdb.query.expr.ToBase64;
import org.araqne.logdb.query.expr.ToBinary;
import org.araqne.logdb.query.expr.ToDate;
import org.araqne.logdb.query.expr.ToDouble;
import org.araqne.logdb.query.expr.ToInt;
import org.araqne.logdb.query.expr.ToIp;
import org.araqne.logdb.query.expr.ToLong;
import org.araqne.logdb.query.expr.ToString;
import org.araqne.logdb.query.expr.Trim;
import org.araqne.logdb.query.expr.Typeof;
import org.araqne.logdb.query.expr.Unique;
import org.araqne.logdb.query.expr.Upper;
import org.araqne.logdb.query.expr.UrlDecode;
import org.araqne.logdb.query.expr.ValueOf;
import org.araqne.logdb.query.expr.Zip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "logdb-function-registry")
@Provides
public class FunctionRegistryImpl implements FunctionRegistry {
	private final Logger slog = LoggerFactory.getLogger(FunctionRegistryImpl.class);

	// function name to factory mappings
	private ConcurrentHashMap<String, FunctionFactory> factories = new ConcurrentHashMap<String, FunctionFactory>();

	private EmbeddedFunctionFactory embedded = new EmbeddedFunctionFactory();

	public FunctionRegistryImpl() {
		registerFactory(embedded);
	}

	@Override
	public Set<String> getFunctionNames() {
		return new HashSet<String>(factories.keySet());
	}

	@Override
	public void registerFactory(FunctionFactory factory) {
		for (String funcName : factory.getFunctionNames()) {
			FunctionFactory old = factories.putIfAbsent(funcName, factory);
			if (old != null)
				slog.warn("araqne logdb: duplicated function [{}:{}] is ignored", factory, funcName);
			else
				slog.debug("araqne logdb: registered function [{}:{}]", factory, funcName);
		}
	}

	@Override
	public void unregisterFactory(FunctionFactory factory) {
		for (String funcName : factory.getFunctionNames()) {
			if (factories.remove(funcName, factory))
				slog.debug("araqne logdb: unregistered function [{}:{}]", factory, funcName);
		}
	}

	private static class EmbeddedFunctionFactory implements FunctionFactory {
		private Map<String, Constructor<?>> constructors = new HashMap<String, Constructor<?>>();

		public EmbeddedFunctionFactory() {
			define("abs", Abs.class);
			define("max", Max.class);
			define("min", Min.class);
			define("case", Case.class);
			define("if", If.class);
			define("concat", Concat.class);
			define("str", ToString.class);
			define("long", ToLong.class);
			define("int", ToInt.class);
			define("double", ToDouble.class);
			define("date", ToDate.class);
			define("epoch", Epoch.class);
			define("string", ToString.class);
			define("left", Left.class);
			define("right", Right.class);
			define("trim", Trim.class);
			define("len", Len.class);
			define("substr", Substr.class);
			define("strreplace", StringReplace.class);
			define("rexreplace", RexReplace.class);
			define("isnull", IsNull.class);
			define("isnotnull", IsNotNull.class);
			define("isnum", IsNum.class);
			define("isstr", IsStr.class);
			define("match", Match.class);
			define("typeof", Typeof.class);
			define("in", In.class);
			define("ip", ToIp.class);
			define("network", Network.class);
			define("urldecode", UrlDecode.class);
			define("lower", Lower.class);
			define("upper", Upper.class);
			define("dateadd", DateAdd.class);
			define("now", Now.class);
			define("datediff", DateDiff.class);
			define("$", ContextReference.class);
			define("guid", Guid.class);
			define("seq", Seq.class);
			define("ip2long", Ip2Long.class);
			define("long2ip", Long2Ip.class);
			define("round", Round.class);
			define("field", Field.class);
			define("floor", Floor.class);
			define("ceil", Ceil.class);
			define("split", Split.class);
			define("array", Array.class);
			define("kvjoin", KvJoin.class);
			define("valueof", ValueOf.class);
			define("datetrunc", DateTrunc.class);
			define("strjoin", StrJoin.class);
			define("hash", Hash.class);
			define("binary", ToBinary.class);
			define("encode", ToBinary.class);
			define("decode", Decode.class);
			define("indexof", IndexOf.class);
			define("contains", Contains.class);
			define("rand", Rand.class);
			define("randbytes", RandBytes.class);
			define("frombase64", FromBase64.class);
			define("tobase64", ToBase64.class);
			define("encrypt", Encrypt.class);
			define("decrypt", Decrypt.class);
			define("zip", Zip.class);
			define("unique", Unique.class);
			define("flatten", Flatten.class);
		}

		private void define(String name, Class<?> clazz) {
			try {
				Constructor<?> c = clazz.getConstructor(QueryContext.class, List.class);
				constructors.put(name, c);
			} catch (Throwable t) {
			}
		}

		@Override
		public Set<String> getFunctionNames() {
			return new HashSet<String>(constructors.keySet());
		}

		@Override
		public Expression newFunction(QueryContext ctx, String name, List<Expression> exprs) {
			Constructor<?> c = constructors.get(name);
			if (c == null)
				throw new QueryParseException("unsupported-function", -1, name);
			try {
				return (Expression) c.newInstance(ctx, exprs);
			} catch (InvocationTargetException e) {
				if (e.getTargetException() instanceof QueryParseException)
					throw (QueryParseException) e.getTargetException();
				else
					throw new QueryParseException("cannot create function instance", -1, e.getTargetException().toString());
			} catch (Throwable t) {
				throw new QueryParseException("cannot create function instance", -1, t.toString());
			}
		}
	}

	@Override
	public Expression newFunction(QueryContext ctx, String functionName, List<Expression> exprs) {
		FunctionFactory ff = factories.get(functionName);
		if (ff == null)
			throw new QueryParseException("unsupported-function", -1, functionName);
		return ff.newFunction(ctx, functionName, exprs);
	}
}
