package org.araqne.logdb.query.command;

import java.io.IOException;
import java.util.Map;

import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryResultSet;
import org.araqne.logdb.QueryService;
import org.araqne.logdb.Row;

public class Result extends DriverQueryCommand {

	private long offset;
	private long limit;
	private Query query;

	public Result(Query query, long offset, long limit) {
		this.query = query;
		this.offset = offset;
		this.limit = limit;
	}

	@Override
	public String getName() {
		return "result";
	}

	@Override
	public void run() {
		QueryResultSet rs = null;
		try {
			rs = query.getResultSet();
			if (offset > 0)
				rs.skip(offset);

			while (rs.hasNext()) {
				Map<String, Object> tuple = rs.next();
				pushPipe(new Row(tuple));
			}

		} catch (IOException e) {
			throw new IllegalStateException("cannot load query #" + query.getId() + " result", e);
		} finally {
			if (rs != null)
				rs.close();
		}
	}

	@Override
	public String toString() {
		String s = "result ";
		if (offset > 0)
			s += " offset=" + offset;

		if (limit > 0)
			s += " limit=" + limit;

		s += " " + query.getId();

		return s;
	}

}
