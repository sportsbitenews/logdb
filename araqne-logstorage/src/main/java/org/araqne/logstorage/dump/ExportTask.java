package org.araqne.logstorage.dump;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.araqne.msgbus.Marshalable;

public class ExportTask implements Marshalable {
	private ExportRequest req;
	private long createdTime;
	private long estimationDoneTime;
	private long completedTime;
	private boolean cancelled;
	private Map<DumpTabletKey, ExportTabletTask> tabletTasks = new ConcurrentHashMap<DumpTabletKey, ExportTabletTask>();

	public ExportTask(ExportRequest req) {
		this.req = req;
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

	public boolean isCancelled() {
		return cancelled;
	}

	public String getGuid() {
		return req.getGuid();
	}

	public ExportRequest getRequest() {
		return req.clone();
	}

	public void setEstimationDone() {
		this.estimationDoneTime = System.currentTimeMillis();
	}

	public void setCompleted() {
		this.completedTime = System.currentTimeMillis();
	}

	public void setCancelled() {
		this.cancelled = true;
	}

	public Map<DumpTabletKey, ExportTabletTask> getTabletTasks() {
		return tabletTasks;
	}

	public void setTabletTasks(Map<DumpTabletKey, ExportTabletTask> tabletTasks) {
		this.tabletTasks = tabletTasks;
	}

	public ExportTask clone() {
		ExportTask c = new ExportTask(req.clone());
		c.createdTime = createdTime;
		c.estimationDoneTime = estimationDoneTime;
		c.completedTime = completedTime;
		c.cancelled = cancelled;
		c.tabletTasks = new ConcurrentHashMap<DumpTabletKey, ExportTabletTask>();
		for (DumpTabletKey key : tabletTasks.keySet()) {
			ExportTabletTask task = tabletTasks.get(key);
			c.tabletTasks.put(key, task.clone());
		}
		return c;
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

		return String.format("guid=%s, created=%s (%d sec), rows=%d/%d (%d%%), tablets=%d/%d", req.getGuid(),
				df.format(createdTime), (elapsed / 1000), actualTotal, estimatedTotal, progressPercent, completedTablet,
				tabletTasks.size());
	}
}
