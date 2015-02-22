package org.araqne.logstorage.dump;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.araqne.msgbus.Marshalable;

public class ExportTask implements Marshalable {
	private String guid;
	private long createdTime;
	private long estimationDoneTime;
	private long completedTime;
	private Map<ExportTableKey, ExportTabletTask> tabletTasks = new ConcurrentHashMap<ExportTableKey, ExportTabletTask>();

	public ExportTask(String guid) {
		this.guid = guid;
		this.createdTime = System.currentTimeMillis();
	}

	public boolean isCompleted() {
		return completedTime > 0;
	}

	public long getCreatedTime() {
		return createdTime;
	}

	public long getEstimationDoneTime() {
		return estimationDoneTime;
	}

	public long getCompletedTime() {
		return completedTime;
	}

	public ExportTask clone() {
		ExportTask c = new ExportTask(guid);
		c.createdTime = createdTime;
		c.estimationDoneTime = estimationDoneTime;
		c.completedTime = completedTime;
		c.tabletTasks = new ConcurrentHashMap<ExportTableKey, ExportTabletTask>();
		for (ExportTableKey key : tabletTasks.keySet()) {
			ExportTabletTask task = tabletTasks.get(key);
			c.tabletTasks.put(key, task.clone());
		}
		return c;
	}

	public String getGuid() {
		return guid;
	}

	public void setGuid(String guid) {
		this.guid = guid;
	}

	public void setEstimationDone() {
		this.estimationDoneTime = System.currentTimeMillis();
	}

	public void setCompleted() {
		this.completedTime = System.currentTimeMillis();
	}

	public Map<ExportTableKey, ExportTabletTask> getTabletTasks() {
		return tabletTasks;
	}

	public void setTabletTasks(Map<ExportTableKey, ExportTabletTask> tabletTasks) {
		this.tabletTasks = tabletTasks;
	}

	@Override
	public Map<String, Object> marshal() {
		long estimatedTotal = 0;
		long actualTotal = 0;
		long completedTablet = 0;

		for (ExportTabletTask s : tabletTasks.values()) {
			estimatedTotal += s.getEstimatedCount();
			actualTotal += s.getActualCount();
			if (s.isCompleted())
				completedTablet++;
		}

		long elapsed = System.currentTimeMillis() - createdTime;

		Map<String, Object> m = new HashMap<String, Object>();
		m.put("created_at", new Date(createdTime));
		m.put("estimation_done_at", estimationDoneTime == 0 ? null : new Date(estimationDoneTime));
		m.put("estimated_total", estimatedTotal);
		m.put("actual_total", actualTotal);
		m.put("completed_tablet", completedTablet);
		m.put("total_tablet", tabletTasks.size());
		m.put("elapsed", elapsed);

		return m;
	}

	@Override
	public String toString() {
		long estimatedTotal = 0;
		long actualTotal = 0;
		long completedTablet = 0;

		for (ExportTabletTask s : tabletTasks.values()) {
			estimatedTotal += s.getEstimatedCount();
			actualTotal += s.getActualCount();
			if (s.isCompleted())
				completedTablet++;
		}

		long elapsed = System.currentTimeMillis() - createdTime;

		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long progressPercent = 0;
		if (estimatedTotal != 0)
			progressPercent = actualTotal * 100 / estimatedTotal;

		return String.format("guid=%s, created=%s (%d sec), rows=%d/%d (%d%%), tablets=%d/%d", guid, df.format(createdTime),
				(elapsed / 1000), actualTotal, estimatedTotal, progressPercent, completedTablet, tabletTasks.size());
	}
}
