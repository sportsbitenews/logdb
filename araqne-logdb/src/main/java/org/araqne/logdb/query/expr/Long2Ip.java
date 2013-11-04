package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Long2Ip implements Expression {
	private final Logger logger = LoggerFactory.getLogger(Long2Ip.class);
	private Expression valueExpr;

	public Long2Ip(List<Expression> exprs) {
		if (exprs.size() != 1)
			throw new LogQueryParseException("invalid-long2ip-args", -1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object eval(LogMap map) {
		Object value = valueExpr.eval(map);
		if (value == null)
			return null;

		if (value instanceof Long)
			return converToIp(((Long) value));
		if (value instanceof Integer)
			return converToIp(((Integer) value));
		if (value instanceof Short)
			return converToIp(((Short) value));

		return null;
	}

	private String converToIp(long targetLong) {
		if (targetLong < 0 || targetLong > 4294967295L)
			return null;

		String result = "";
		for (int count = 24; count >= 0; count -= 8) {
			long part = (targetLong >> count) & 0xff;
			result += part;
			if (count != 0)
				result += ".";
		}

		return result;
	}
}
