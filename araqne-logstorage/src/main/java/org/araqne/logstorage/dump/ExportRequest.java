package org.araqne.logstorage.dump;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class ExportRequest {
	private String driverType;
	private String guid = UUID.randomUUID().toString();
	private Set<String> tableNames;
	private Date from;
	private Date to;
	private Map<String, String> params;

	public ExportRequest(String driverType, Set<String> tableNames, Date from, Date to, Map<String, String> params) {
		this.driverType = driverType;
		this.tableNames = tableNames;
		this.from = from;
		this.to = to;
		this.params = params;
	}

	public String getDriverType() {
		return driverType;
	}

	public void setDriverType(String driverName) {
		this.driverType = driverName;
	}

	public String getGuid() {
		return guid;
	}

	public Set<String> getTableNames() {
		return tableNames;
	}

	public Date getFrom() {
		return from;
	}

	public Date getTo() {
		return to;
	}

	public Map<String, String> getParams() {
		return params;
	}
}
