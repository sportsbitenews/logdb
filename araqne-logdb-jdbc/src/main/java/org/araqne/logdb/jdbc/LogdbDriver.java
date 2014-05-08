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
package org.araqne.logdb.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import org.araqne.logdb.client.LogDbClient;

public class LogdbDriver implements Driver {

	static final String PREFIX1 = "logdb://";
	static final String PREFIX2 = "logpresso://";

	static {
		try {
			DriverManager.registerDriver(new LogdbDriver());
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		return url.startsWith(PREFIX1) || url.startsWith(PREFIX2);
	}

	@SuppressWarnings("resource")
	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		LogDbClient client = null;
		String host = "localhost";
		int port = 8888;
		String loginName = "araqne";
		String password = "";

		try {
			client = new LogDbClient();
			client.connect(host, port, loginName, password);
			return new LogdbConnection(client);
		} catch (IOException e) {
			throw new SQLException(e);
		}
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return null;
	}

	@Override
	public int getMajorVersion() {
		return 0;
	}

	@Override
	public int getMinorVersion() {
		return 11;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}

}
