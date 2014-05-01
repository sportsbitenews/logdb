package org.araqne.logdb.jdbc;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import org.araqne.logdb.client.LogCursor;

public class LogdbResultSet extends AbstractResultSet {

	private LogCursor cursor;
	private Map<String, Object> current;

	LogdbResultSet(LogCursor cursor) {
		this.cursor = cursor;
	}

	@Override
	public boolean next() throws SQLException {
		if (!cursor.hasNext())
			return false;

		current = cursor.next();
		return true;
	}

	@Override
	public void close() throws SQLException {
		try {
			cursor.close();
		} catch (IOException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return new LogdbResultSetMetaData();
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		Object v = current.get("line");
		return v != null ? v.toString() : null;
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		Object v = current.get("line");
		return v != null ? v.toString() : null;
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		Object v = current.get(columnLabel);
		return v != null ? v.toString() : null;
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		Object v = current.get(columnLabel);
		return v != null ? v.toString() : null;
	}

}
