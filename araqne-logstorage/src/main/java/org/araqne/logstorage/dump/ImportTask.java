package org.araqne.logstorage.dump;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ImportTask {

	private String guid;
	private long createdTime;
	private long completedTime;
	private boolean cancelled;
	private Map<ExportTableKey, ImportTabletTask> tabletTasks = new ConcurrentHashMap<ExportTableKey, ImportTabletTask>();

	public ImportTask(String guid) {
		this.guid = guid;
		this.createdTime = System.currentTimeMillis();
	}

	public String getGuid() {
		return guid;
	}

	public void setGuid(String guid) {
		this.guid = guid;
	}

	public long getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(long createdTime) {
		this.createdTime = createdTime;
	}

	public long getCompletedTime() {
		return completedTime;
	}

	public void setCompletedTime(long completedTime) {
		this.completedTime = completedTime;
	}

	public boolean isCancelled() {
		return cancelled;
	}

	public void setCancelled() {
		this.cancelled = true;
	}

	public Map<ExportTableKey, ImportTabletTask> getTabletTasks() {
		return tabletTasks;
	}

	public void setTabletTasks(Map<ExportTableKey, ImportTabletTask> tabletTasks) {
		this.tabletTasks = tabletTasks;
	}

	public ImportTask clone() {
		ImportTask c = new ImportTask(guid);
		c.createdTime = createdTime;
		c.completedTime = completedTime;
		c.cancelled = cancelled;
		c.tabletTasks = new ConcurrentHashMap<ExportTableKey, ImportTabletTask>();
		for (ExportTableKey key : tabletTasks.keySet()) {
			ImportTabletTask task = tabletTasks.get(key);
			c.tabletTasks.put(key, task.clone());
		}
		
		return c;
	}

}
