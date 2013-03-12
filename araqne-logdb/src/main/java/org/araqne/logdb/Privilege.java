package org.araqne.logdb;

import java.util.ArrayList;
import java.util.List;

public class Privilege {
	private String loginName;
	private String tableName;
	private List<Permission> permissions = new ArrayList<Permission>();

	public Privilege() {
	}

	public Privilege(String loginName, String tableName, List<Permission> permissions) {
		this.loginName = loginName;
		this.tableName = tableName;
		this.permissions = permissions;
	}

	public String getLoginName() {
		return loginName;
	}

	public void setLoginName(String loginName) {
		this.loginName = loginName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<Permission> getPermissions() {
		return permissions;
	}

	public void setPermissions(List<Permission> permissions) {
		this.permissions = permissions;
	}
}
