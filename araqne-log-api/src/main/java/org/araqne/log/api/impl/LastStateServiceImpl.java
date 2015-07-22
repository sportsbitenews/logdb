/*
 * Copyright 2013 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.log.api.impl;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.api.PrimitiveConverter;
import org.araqne.log.api.LastState;
import org.araqne.log.api.LastStateListener;
import org.araqne.log.api.LastStateService;
import org.json.JSONConverter;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage last state of logger. Each logger state is persist as single JSON
 * file.
 * 
 * @author xeraph
 * @since 2.9.0
 */
@Component(name = "last-state-service")
@Provides
public class LastStateServiceImpl implements LastStateService {
	private final Logger slog = LoggerFactory.getLogger(LastStateServiceImpl.class);

	/**
	 * last state JSON directory
	 */
	private File dir;

	/**
	 * in-memory last state cache
	 */
	private ConcurrentMap<String, LastState> states = new ConcurrentHashMap<String, LastState>();

	private CopyOnWriteArraySet<LastStateListener> listeners = new CopyOnWriteArraySet<LastStateListener>();

	/**
	 * last state file sync in batch mode
	 */
	private FileSyncThread sync;

	/**
	 * reject last state update while closing service
	 */
	private volatile boolean reject;

	@Validate
	public void start() {
		reject = false;
		ensureRepository();

		// load all states
		loadAllFiles();

		// start sync thread
		sync = new FileSyncThread();
		sync.start();
	}

	private void loadAllFiles() {
		File[] l = dir.listFiles();
		if (l == null)
			return;

		for (File f : l) {
			// file name format is namespace$name.state or
			// node$namespace$name.state
			if (!f.getName().endsWith(".state") || !f.getName().contains("$"))
				continue;

			try {
				LastState s = readStateFile(f);
				states.put(s.getLoggerName(), s);
			} catch (IOException e) {
				slog.error("araqne log api: cannot load last state file [" + f.getAbsolutePath() + "]", e);
			}
		}
	}

	private void ensureRepository() {
		String path = System.getProperty("araqne.logapi.state.dir");
		if (path != null)
			dir = new File(path);
		else
			dir = new File(System.getProperty("araqne.data.dir"), "araqne-log-api/state");
		if (dir.mkdirs())
			slog.info("araqne log api: last state repository [{}] is created", dir.getAbsolutePath());
	}

	@Invalidate
	public void stop() {
		reject = true;

		// wait until file sync is done
		try {
			sync.doStop = true;
			sync.join();
			sync = null;

			slog.info("araqne log api: state thread ended");
		} catch (InterruptedException e) {
			slog.warn("araqne log api: last state sync thread join interrupted", e);
		}

	}

	@Override
	public List<LastState> getStates() {
		return new ArrayList<LastState>(states.values());
	}

	@Override
	public LastState getState(String name) {
		LastState old = states.get(name);
		if (old == null)
			return null;

		return LastState.cloneState(old);
	}

	@Override
	public void setState(LastState state) {
		if (state == null)
			throw new IllegalArgumentException("last state should not be null");

		if (reject)
			throw new IllegalStateException("cannot update last state of logger [" + state.getLoggerName()
					+ "], service is closing");

		// skip disk update if state is not changed at all
		state = LastState.cloneState(state);
		LastState old = states.get(state.getLoggerName());
		if (old != null && old.equals(state)) {
			slog.debug("araqne log api: logger [{}] same state for update", state.getLoggerName());
			return;
		}

		// update count can be assigned from caller
		if (old != null && state.getUpdateCount() == 0)
			state.setUpdateCount(old.getUpdateCount() + 1);

		states.put(state.getLoggerName(), state);

		// queue disk sync
		while (true) {
			try {
				sync.queue.put(state);
				break;
			} catch (InterruptedException e) {
				slog.debug("araqne log api: interrupted last state update of logger [{}]", state.getLoggerName());
			}
		}
	}

	@Override
	public void deleteState(String loggerName) {
		DeleteState state = new DeleteState();
		state.setLoggerName(loggerName);

		// queue disk sync
		try {
			sync.queue.put(state);
		} catch (InterruptedException e) {
			slog.warn("araqne log api: interrupted last state update of logger [{}]", state.getLoggerName());
		}

		states.remove(loggerName);
	}

	@Override
	public void addListener(LastStateListener listener) {
		listeners.add(listener);
	}

	@Override
	public void removeListener(LastStateListener listener) {
		listeners.remove(listener);
	}

	private class DeleteState extends LastState {

	}

	private class FileSyncThread extends Thread {
		private boolean doStop = false;

		// fairness is required for update ordering
		private ArrayBlockingQueue<LastState> queue = new ArrayBlockingQueue<LastState>(1000, true);

		public FileSyncThread() {
			super("Last State Sync");
		}

		@Override
		public void run() {
			try {
				slog.info("araqne log api: last state sync thread started");
				// must do all flush job when stop signal'ed
				while (!doStop || !queue.isEmpty()) {
					try {
						ArrayList<LastState> l = new ArrayList<LastState>();

						// block waiting with timeout
						LastState s = queue.poll(1, TimeUnit.SECONDS);
						if (s != null) {
							l.add(s);
							queue.drainTo(l);
							syncFiles(l);

							slog.debug("araqne log api: sync'ed [{}] state", l.size());
						}
					} catch (Throwable e) {
						slog.debug("araqne log api: last state sync error", e);
					}
				}
			} finally {
				slog.info("araqne log api: last state sync thread exit");
			}
		}

		private void syncFiles(List<LastState> l) {
			// merge state by logger name
			Map<String, LastState> merge = new HashMap<String, LastState>();
			for (LastState s : l) {
				merge.put(s.getLoggerName(), s);
			}

			// flush files
			for (LastState s : merge.values()) {
				File f = getFilePath(s);
				if (s instanceof DeleteState)
					f.delete();
				else
					writeStateFile(s, f);
			}
		}
	}

	private File getFilePath(LastState s) {
		String fileName = s.getLoggerName().replaceAll("\\\\", "\\$") + ".state";
		return new File(dir, fileName);
	}

	private LastState readStateFile(File f) throws IOException {
		FileInputStream fis = null;
		BufferedInputStream bis = null;
		try {
			fis = new FileInputStream(f);
			bis = new BufferedInputStream(fis);
			JSONTokener tokener = new JSONTokener(new InputStreamReader(bis, "utf-8"));
			Map<String, Object> m = JSONConverter.parse(new JSONObject(tokener));
			if (m.get("last_log_date") != null) {
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
				Date d = df.parse((String) m.get("last_log_date"), new ParsePosition(0));
				m.put("last_log_date", d);
			}

			return PrimitiveConverter.parse(LastState.class, m);
		} catch (ClassCastException e) {
			throw new IOException("invalid json file [" + f.getAbsolutePath() + "]", e);
		} catch (JSONException e) {
			throw new IOException("invalid json file [" + f.getAbsolutePath() + "]", e);
		} finally {
			ensureClose(bis);
			ensureClose(fis);
		}
	}

	private void writeStateFile(LastState state, File f) {
		// write tmp file first, and rename it
		File tmp = new File(f.getAbsolutePath() + ".tmp");
		FileOutputStream fos = null;
		JSONWriter writer = null;
		OutputStreamWriter ow = null;
		BufferedWriter bw = null;
		boolean success = false;

		try {
			fos = new FileOutputStream(tmp);
			ow = new OutputStreamWriter(fos, "utf-8");
			bw = new BufferedWriter(ow);
			writer = new JSONWriter(bw);
			JSONConverter.jsonize(PrimitiveConverter.serialize(state), writer);
			success = true;
		} catch (JSONException e) {
			slog.error("araqne log api: cannot jsonize state [{}]", state.getLoggerName(), f.getAbsolutePath());
		} catch (Throwable e) {
			slog.error(
					"araqne log api: cannot write state [" + state.getLoggerName() + "] to file [" + f.getAbsolutePath() + "]", e);
		} finally {
			ensureFlush(bw);
			ensureFlush(ow);
			ensureFsync(fos);

			ensureClose(bw);
			ensureClose(ow);
			ensureClose(fos);
		}

		// prevent broken file writing caused by low disk space
		if (success) {
			boolean rename = (f.delete() || !f.exists()) && tmp.renameTo(f);
			if (!rename) {
				slog.error("araqne log api: cannot delete last state file [{}] to [{}]", tmp.getAbsolutePath(),
						f.getAbsolutePath());
				tmp.delete();
			}
		} else {
			tmp.delete();
		}
	}

	/**
	 * enforce fsync for ext4 (zero-length file problem)
	 * 
	 * @see http://lwn.net/Articles/323169/
	 */
	private void ensureFsync(FileOutputStream o) {
		if (o != null) {
			try {
				o.flush();
			} catch (Throwable t) {
			}

			try {
				o.getFD().sync();
			} catch (Throwable t) {
			}
		}
	}

	private void ensureFlush(Writer w) {
		if (w != null) {
			try {
				w.flush();
			} catch (Throwable t) {
			}
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
}
