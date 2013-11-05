package org.araqne.logdb.query.expr;

import java.util.List;

import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Ip2Long implements Expression {
	private final Logger logger = LoggerFactory.getLogger(Ip2Long.class);
	private Expression valueExpr;

	public Ip2Long(List<Expression> exprs) {
		if (exprs.size() != 1)
			throw new LogQueryParseException("invalid-ip2long-args", -1);
		this.valueExpr = exprs.get(0);
	}

	@Override
	public Object eval(LogMap map) {
		Object value = valueExpr.eval(map);
		if (value == null)
			return null;

		return convertToLong(value.toString());
	}

	private Long convertToLong(String targetIp) {
		String[] ipParts = targetIp.split("\\.");
		if (ipParts.length != 4)
			return null;

		try {
			long result = 0;
			for (String ipPart : ipParts) {
				int part = Integer.parseInt(ipPart);
				if (part < 0 || part > 255)
					return null;
				result <<= 8;
				result |= part;
			}
			return result;
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: ip2long failed, value " + targetIp, t);
			return null;
		}
	}

}
