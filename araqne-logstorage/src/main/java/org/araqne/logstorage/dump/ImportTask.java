package org.araqne.logstorage.dump;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ImportTask {

	private ImportRequest req;
	private long createdTime;
	private long completedTime;
	private boolean cancelled;
	private Map<DumpTabletKey, ImportTabletTask> tabletTasks = new ConcurrentHashMap<DumpTabletKey, ImportTabletTask>();

	public ImportTask(ImportRequest req) {
		this.req = req;
		this.createdTime = System.currentTimeMillis();
	}

	public boolean isCompleted() {
		return completedTime > 0;
	}

	public String getGuid() {
		return req.getGuid();
	}

	public ImportRequest getRequest() {
		return req.clone();
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

	public void setCompleted() {
		this.completedTime = System.currentTimeMillis();
	}

	public Map<DumpTabletKey, ImportTabletTask> getTabletTasks() {
		return tabletTasks;
	}

	public void setTabletTasks(Map<DumpTabletKey, ImportTabletTask> tabletTasks) {
		this.tabletTasks = tabletTasks;
	}

	public ImportTask clone() {
		ImportTask c = new ImportTask(req.clone());
		c.createdTime = createdTime;
		c.completedTime = completedTime;
		c.cancelled = cancelled;
		c.tabletTasks = new ConcurrentHashMap<DumpTabletKey, ImportTabletTask>();
		for (DumpTabletKey key : tabletTasks.keySet()) {
			ImportTabletTask task = tabletTasks.get(key);
			c.tabletTasks.put(key, task.clone());
		}

		return c;
	}

	@Override
	public String toString() {
		long totalCount = 0;
		long importCount = 0;
		long completedTablet = 0;

		for (ImportTabletTask s : tabletTasks.values()) {
			totalCount += s.getTotalCount();
			importCount += s.getImportCount();
			if (s.isCompleted())
				completedTablet++;
		}

		long elapsed = System.currentTimeMillis() - createdTime;

		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long progressPercent = 0;
		if (totalCount != 0)
			progressPercent = importCount * 100 / totalCount;

		return String.format("guid=%s, created=%s (%d sec), rows=%d/%d (%d%%), tablets=%d/%d", req.getGuid(),
				df.format(createdTime), (elapsed / 1000), importCount, totalCount, progressPercent, completedTablet,
				tabletTasks.size());
	}

}
