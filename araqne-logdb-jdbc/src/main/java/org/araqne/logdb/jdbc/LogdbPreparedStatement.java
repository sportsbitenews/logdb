package org.araqne.logdb.jdbc;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.araqne.logdb.client.LogCursor;
import org.araqne.logdb.client.LogDbClient;

public class LogdbPreparedStatement extends AbstractPreparedStatement {
	private LogDbClient client;
	private LogCursor cursor;
	private String sql;

	public LogdbPreparedStatement(LogDbClient client, String sql, int type, int concurrency, int holdability) {
		this.client = client;
		this.sql = sql;
	}

	@Override
	public ResultSet executeQuery() throws SQLException {
		try {
			cursor = client.query(sql);
			return new LogdbResultSet(cursor);
		} catch (IOException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public void close() throws SQLException {
		try {
			cursor.close();
		} catch (Throwable t) {
		}
	}
}
