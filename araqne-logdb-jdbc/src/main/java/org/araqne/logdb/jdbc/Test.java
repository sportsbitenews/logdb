package org.araqne.logdb.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Test {
	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		Class.forName("org.araqne.logdb.jdbc.LogdbDriver");
		Connection conn = DriverManager.getConnection("logpresso://localhost:8888");
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("table limit=10 iis");

		while (rs.next()) {
			System.out.println(rs.getString("line"));
		}

		conn.close();
	}
}
