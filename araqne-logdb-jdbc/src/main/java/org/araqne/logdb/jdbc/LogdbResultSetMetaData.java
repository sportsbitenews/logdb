package org.araqne.logdb.jdbc;

import java.sql.SQLException;
import java.sql.Types;

public class LogdbResultSetMetaData extends AbstractResultSetMetaData {

	@Override
	public int getColumnCount() throws SQLException {
		return 1;
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return "line";
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return "line";
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		return Types.NVARCHAR;
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		return "NVARCHAR";
	}

}
