package org.araqne.logstorage.dump;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class ExportRequest {
	private String driverName;
	private String guid = UUID.randomUUID().toString();
	private Set<String> tableNames;
	private Date from;
	private Date to;
	private Map<String, String> params;

	public ExportRequest(String driverName, Set<String> tableNames, Date from, Date to, Map<String, String> params) {
		this.driverName = driverName;
		this.tableNames = tableNames;
		this.from = from;
		this.to = to;
		this.params = params;
	}

	public String getDriverName() {
		return driverName;
	}

	public void setDriverName(String driverName) {
		this.driverName = driverName;
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
