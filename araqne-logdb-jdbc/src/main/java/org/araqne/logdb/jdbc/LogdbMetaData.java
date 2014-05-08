package org.araqne.logdb.jdbc;

import java.sql.SQLException;

public class LogdbMetaData extends AbstractDbMetaData {

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		return "";
	}
}
