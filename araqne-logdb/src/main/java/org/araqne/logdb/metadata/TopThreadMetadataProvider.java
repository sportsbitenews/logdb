package org.araqne.logdb.metadata;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.FieldOrdering;
import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.MetadataCallback;
import org.araqne.logdb.MetadataProvider;
import org.araqne.logdb.MetadataService;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.Row;

@Component(name = "logdb-topthread-metadata")
public class TopThreadMetadataProvider implements MetadataProvider, FieldOrdering {
	@Requires
	private MetadataService metadataService;

	@Requires
	private FunctionRegistry functionRegistry;

	@Validate
	public void start() {
		metadataService.addProvider(this);
	}

	@Invalidate
	public void stop() {
		if (metadataService != null)
			metadataService.removeProvider(this);
	}

	@Override
	public List<String> getFieldOrder() {
		return Arrays.asList("tid", "name", "state", "priority", "usage", "stacktrace");
	}

	@Override
	public String getType() {
		return "topthreads";
	}

	@Override
	public void verify(QueryContext context, String queryString) {
		if (!context.getSession().isAdmin())
			throw new QueryParseException("95050", -1, -1, null);

		ThreadMXBean bean = ManagementFactory.getThreadMXBean();
		if (!bean.isThreadCpuTimeSupported())
			throw new QueryParseException("95051", -1, -1, null);

		if (!bean.isThreadCpuTimeEnabled())
			throw new QueryParseException("95051", -1, -1, null);
	}

	@Override
	public void query(QueryContext context, String queryString, MetadataCallback callback) {
		ThreadMXBean bean = ManagementFactory.getThreadMXBean();

		ArrayList<ThreadCpuUsage> usages = new ArrayList<ThreadCpuUsage>();

		for (long tid : bean.getAllThreadIds()) {
			long time = bean.getThreadCpuTime(tid);
			usages.add(new ThreadCpuUsage(tid, time));
		}

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}

		Map<Thread, StackTraceElement[]> stacks = Thread.getAllStackTraces();

		for (long tid : bean.getAllThreadIds()) {
			ThreadCpuUsage usage = find(usages, tid);
			if (usage != null)
				usage.secondTime = bean.getThreadCpuTime(tid);
		}

		Collections.sort(usages);

		for (ThreadCpuUsage usage : usages) {
			long elapsed = usage.secondTime - usage.firstTime;
			// remove just created thread or sleeping threads (noisy)
			if (elapsed <= 0)
				continue;

			StringBuilder sb = new StringBuilder();
			Thread t = findThread(stacks, usage.tid);
			if (t == null)
				continue;

			StackTraceElement[] stack = findStack(stacks, usage.tid);
			for (StackTraceElement el : stack) {
				sb.append(String.format("%s.%s %s\n", el.getClassName(), el.getMethodName(), getFileAndLineNumber(el)));
			}

			Map<String, Object> m = new HashMap<String, Object>();
			m.put("tid", t.getId());
			m.put("name", t.getName());
			m.put("state", t.getState().toString());
			m.put("priority", t.getPriority());
			m.put("usage", elapsed);
			m.put("stacktrace", sb.toString());

			callback.onPush(new Row(m));
		}
	}

	private ThreadCpuUsage find(List<ThreadCpuUsage> usages, long tid) {
		for (ThreadCpuUsage usage : usages)
			if (usage.tid == tid)
				return usage;

		return null;
	}

	private Thread findThread(Map<Thread, StackTraceElement[]> stacks, long tid) {
		for (Thread t : stacks.keySet())
			if (t.getId() == tid)
				return t;

		return null;
	}

	private StackTraceElement[] findStack(Map<Thread, StackTraceElement[]> stacks, long tid) {
		for (Thread t : stacks.keySet())
			if (t.getId() == tid)
				return stacks.get(t);

		return null;
	}

	private String getFileAndLineNumber(StackTraceElement el) {
		if (el.getFileName() != null && el.getLineNumber() > 0)
			return String.format("(%s:%d)", el.getFileName(), el.getLineNumber());
		else if (el.getFileName() != null && el.getLineNumber() <= 0)
			return String.format("(%s)", el.getFileName());
		else
			return "";
	}

	private static class ThreadCpuUsage implements Comparable<ThreadCpuUsage> {
		private long tid;
		private long firstTime;
		private long secondTime;

		public ThreadCpuUsage(long tid, long firstTime) {
			this.tid = tid;
			this.firstTime = firstTime;
		}

		@Override
		public int compareTo(ThreadCpuUsage o) {
			// descending order
			long self = secondTime - firstTime;
			long other = o.secondTime - o.firstTime;
			return (int) (other - self);
		}
	}
}
