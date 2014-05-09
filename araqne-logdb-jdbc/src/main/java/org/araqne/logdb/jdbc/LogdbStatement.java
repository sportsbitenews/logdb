package org.araqne.logdb.jdbc;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.araqne.logdb.client.LogCursor;
import org.araqne.logdb.client.LogDbClient;

public class LogdbStatement extends AbstractStatement {
	private LogDbClient client;
	private LogCursor cursor;

	public LogdbStatement(LogDbClient client, int type, int concurrency, int holdability) {
		this.client = client;
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
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
