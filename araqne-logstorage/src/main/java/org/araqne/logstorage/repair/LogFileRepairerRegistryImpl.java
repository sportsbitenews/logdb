package org.araqne.logstorage.repair;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logstorage.LogFileRepairer;
import org.araqne.logstorage.LogFileRepairerRegistry;
import org.araqne.logstorage.LogFileRepairerService;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "logstorage-log-file-repairer-registry")
@Provides
public class LogFileRepairerRegistryImpl implements LogFileRepairerRegistry {
	private final static Logger logger = LoggerFactory.getLogger(LogFileRepairerRegistryImpl.class);

	private BundleContext bc;
	private ConcurrentHashMap<String, WaitEvent> availableRepairers = new ConcurrentHashMap<String, WaitEvent>();

	private ConcurrentMap<String, LogFileRepairerService> repairers = new ConcurrentHashMap<String, LogFileRepairerService>();

	public LogFileRepairerRegistryImpl(BundleContext bc) {
		this.bc = bc;

		String opt = System.getProperty("araqne.logstorage.repair");
		if (opt != null && (opt.equalsIgnoreCase("disabled") || !Boolean.parseBoolean(opt)))
			bc.registerService(IntegrityChecker.class.getName(), new DummyIntegrityChecker(), null);
	}

	@Override
	public void register(LogFileRepairerService repairer) {
		String type = repairer.getType();
		logger.info("araqne logstorage: loaded file repairer [{}]", type);
		repairers.put(type, repairer);

		WaitEvent ev = new WaitEvent(type, true);
		WaitEvent old = availableRepairers.putIfAbsent(type, ev);
		if (old != null) {
			old.setReady();
		} else {
			rewriteRepairerListFile();
		}
	}

	@Override
	public void unregister(LogFileRepairerService repairer) {
		String type = repairer.getType();

		WaitEvent ev = availableRepairers.get(type);
		if (ev == null)
			throw new UnsupportedOperationException("not supported repairer: " + type);

		ev.ready = false;
		repairers.remove(type);

		logger.info("araqne logstorage: unloaded file repairer [{}]", type);
	}

	@Override
	public void uninstall(String type) {
		WaitEvent ev = availableRepairers.remove(type);
		if (ev == null)
			throw new IllegalStateException("not installed repairer: " + type);

		rewriteRepairerListFile();
	}

	@Override
	public String[] getRepairerTypes() {
		return availableRepairers.keySet().toArray(new String[0]);
	}

	@Override
	public String[] getInstalledTypes() {
		return availableRepairers.keySet().toArray(new String[0]);
	}

	@Override
	public LogFileRepairer newRepairer(String type) {
		WaitEvent ev = availableRepairers.get(type);
		if (ev == null)
			throw new UnsupportedOperationException("not supported repairer: " + type);

		ev.await();

		LogFileRepairerService logFileRepairerService = repairers.get(type);
		return logFileRepairerService.newRepairer();
	}

	@Validate
	public void start() throws IOException {
		File f = getRepairerListFile();
		if (!f.exists())
			return;

		// load file engine list
		FileInputStream is = null;
		BufferedReader br = null;
		try {
			is = new FileInputStream(f);
			br = new BufferedReader(new InputStreamReader(is, "utf-8"));

			while (true) {
				String line = br.readLine();
				if (line == null)
					break;

				String type = line.trim();
				availableRepairers.put(type, new WaitEvent(type));
			}
		} finally {
			ensureClose(br);
			ensureClose(is);
		}
	}

	private void ensureClose(Closeable c) {
		if (c != null) {
			try {
				c.close();
			} catch (IOException e) {
			}
		}
	}

	private File getRepairerListFile() {
		return bc.getDataFile("repairer.lst");
	}

	private void rewriteRepairerListFile() {
		// update engine list file
		FileOutputStream fos = null;
		BufferedWriter bw = null;

		try {
			File f = getRepairerListFile();
			f.delete();

			fos = new FileOutputStream(f);
			bw = new BufferedWriter(new OutputStreamWriter(fos, "utf-8"));

			for (String name : availableRepairers.keySet())
				bw.write(name + "\n");

		} catch (IOException e) {
			logger.error("araqne logstorage: cannot update engine list", e);
		} finally {
			if (bw != null) {
				try {
					bw.close();
				} catch (IOException e) {
				}
			}

			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
				}
			}
		}
	}

	// XXX : copy from LogFileServiceRegistryImpl
	private static class WaitEvent {
		private String type;
		private Lock lock = new ReentrantLock();
		private Condition cond = lock.newCondition();
		private volatile boolean ready;

		public WaitEvent(String type) {
			this(type, false);
		}

		public WaitEvent(String type, boolean ready) {
			this.type = type;
			this.ready = ready;
		}

		public void await() {
			if (ready)
				return;

			lock.lock();
			try {
				while (!ready) {
					try {
						logger.info("araqne logstorage: waiting file engine [{}]", type);
						cond.await();
					} catch (InterruptedException e) {
					}
				}
			} finally {
				lock.unlock();
			}

			logger.info("araqne logstorage: file engine [{}] ready!", type);
		}

		public void setReady() {
			ready = true;
			lock.lock();
			try {
				cond.signalAll();
			} finally {
				lock.unlock();
			}
		}
	}

	private class DummyIntegrityChecker implements IntegrityChecker {
	}
}
