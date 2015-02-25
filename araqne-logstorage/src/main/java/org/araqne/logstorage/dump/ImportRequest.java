package org.araqne.logstorage.dump;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ImportRequest {
	private String driverName;
	private String guid = UUID.randomUUID().toString();
	private Map<String, String> params;
	private List<DumpTabletEntry> entries;

	public String getDriverName() {
		return driverName;
	}

	public void setDriverName(String driverName) {
		this.driverName = driverName;
	}

	public String getGuid() {
		return guid;
	}

	public void setGuid(String guid) {
		this.guid = guid;
	}

	public Map<String, String> getParams() {
		return params;
	}

	public void setParams(Map<String, String> params) {
		this.params = params;
	}

	public List<DumpTabletEntry> getEntries() {
		return entries;
	}

	public void setEntries(List<DumpTabletEntry> entries) {
		this.entries = entries;
	}
}
