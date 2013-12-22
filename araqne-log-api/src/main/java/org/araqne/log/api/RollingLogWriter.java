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
package org.araqne.log.api;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

public class RollingLogWriter extends AbstractLogger implements LoggerRegistryEventListener {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(RollingLogWriter.class);
	private final File file;
	private final long maxFileSize;
	private final int maxBackupIndex;
	private final String charsetName;

	private LoggerRegistry loggerRegistry;

	/**
	 * full name of data source logger
	 */
	private String loggerName;

	private BufferedOutputStream bos;
	private FileOutputStream fos;
	private long totalBytes;
	private boolean noRollingMode;

	private Receiver receiver = new Receiver();

	public RollingLogWriter(LoggerSpecification spec, LoggerFactory factory, LoggerRegistry loggerRegistry) {
		super(spec, factory);
		this.loggerRegistry = loggerRegistry;
		Map<String, String> config = spec.getConfig();
		this.loggerName = config.get("source_logger");
		this.file = new File(config.get("file_path"));
		this.maxFileSize = Long.parseLong(config.get("max_file_size"));

		String s = config.get("max_backup_index");
		this.maxBackupIndex = s != null && !s.isEmpty() ? Integer.parseInt(s) : 1;

		s = config.get("charset");
		this.charsetName = s != null ? s : "utf-8";
	}

	public void flush() {
		if (bos != null) {
			try {
				bos.flush();
				fos.getFD().sync();
			} catch (IOException e) {
			}
		}
	}

	@Override
	protected void onStart() {
		ensureOpen();

		loggerRegistry.addListener(this);
		Logger logger = loggerRegistry.getLogger(loggerName);

		if (logger != null) {
			slog.debug("araqne log api: connect pipe to source logger [{}]", loggerName);
			logger.addLogPipe(receiver);
		} else
			slog.debug("araqne log api: source logger [{}] not found", loggerName);
	}

	@Override
	protected void onStop() {
		ensureClose();

		try {
			if (loggerRegistry != null) {
				Logger logger = loggerRegistry.getLogger(loggerName);
				if (logger != null) {
					slog.debug("araqne log api: disconnect pipe from source logger [{}]", loggerName);
					logger.removeLogPipe(receiver);
				}

				loggerRegistry.removeListener(this);
			}
		} catch (Throwable t) {
			slog.debug("araqne log api: cannot remove logger [" + getFullName() + "] from registry", t);
		}
	}

	private void ensureOpen() {
		if (file.getParentFile().mkdirs())
			slog.info("araqne log api: created parent directory [{}] by rolling log file transformer", file.getParentFile()
					.getAbsolutePath());

		if (file.exists())
			totalBytes = file.length();

		try {
			fos = new FileOutputStream(file, true);
			bos = new BufferedOutputStream(fos);
		} catch (IOException e) {
			throw new IllegalStateException("cannot open rolling logger [" + getFullName() + "]", e);
		}
	}

	private void ensureClose() {
		slog.debug("araqne log api: closing output file of rolling logger [{}]", getFullName());

		try {
			if (bos != null) {
				bos.close();
				bos = null;
			}
		} catch (Throwable t) {
		}

		try {
			if (fos != null) {
				fos.close();
				fos = null;
			}
		} catch (Throwable t) {
		}
	}

	@Override
	public boolean isPassive() {
		return true;
	}

	@Override
	protected void runOnce() {
	}

	@Override
	public void loggerAdded(Logger logger) {
		if (logger.getFullName().equals(loggerName)) {
			slog.debug("araqne log api: source logger [{}] loaded", loggerName);
			logger.addLogPipe(receiver);
		}
	}

	@Override
	public void loggerRemoved(Logger logger) {
		if (logger.getFullName().equals(loggerName)) {
			slog.debug("araqne log api: source logger [{}] unloaded", loggerName);
			logger.removeLogPipe(receiver);
		}
	}

	private boolean rollFile() {
		for (int num = maxBackupIndex; num > 0; num--) {
			File fromPath = new File(file.getAbsolutePath() + "." + num);
			if (!fromPath.exists())
				continue;

			if (num == maxBackupIndex) {

				int tryCount = 0;
				while (!fromPath.delete()) {
					try {
						if (tryCount++ > 6000) {
							slog.debug("araqne log api: failed to delete [{}] at logger [{}]", fromPath.getAbsolutePath(),
									getFullName());

							return false;
						}

						Thread.sleep(100);
					} catch (InterruptedException e) {
					}
				}
				continue;
			}

			File toPath = new File(file.getAbsolutePath() + "." + (num + 1));

			slog.debug("araqne log api: try to rename [{}] to [{}] at logger [{}]", new Object[] { fromPath.getAbsolutePath(),
					toPath.getAbsolutePath(), getFullName() });

			int tryCount = 0;
			while (!fromPath.renameTo(toPath)) {
				try {
					if (tryCount++ > 6000) {
						slog.debug("araqne log api: failed to rename [{}] to [{}] at logger [{}]",
								new Object[] { fromPath.getAbsolutePath(), toPath.getAbsolutePath(), getFullName() });
						return false;
					}

					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
			}
		}

		// close file stream, rename and reopen
		File toPath = new File(file.getAbsolutePath() + ".1");
		ensureClose();

		int tryCount = 0;
		slog.debug("araqne log api: try to rename [{}] to [{}] at logger [{}]",
				new Object[] { file.getAbsolutePath(), toPath.getAbsolutePath(), getFullName() });

		while (!file.renameTo(toPath)) {
			try {
				if (tryCount++ > 6000) {
					slog.debug("araqne log api: failed to rename [{}] to [{}] at logger [{}]",
							new Object[] { file.getAbsolutePath(), toPath.getAbsolutePath(), getFullName() });

					return false;
				}

				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}

		ensureOpen();

		return true;
	}

	private class Receiver extends AbstractLogPipe {
		@Override
		public void onLog(Logger logger, Log log) {
			Map<String, Object> params = log.getParams();
			String line = (String) params.get("line");
			if (line == null)
				return;

			write(new SimpleLog(log.getDate(), getFullName(), params));

			try {
				byte[] b = line.getBytes(charsetName);
				if (maxFileSize < totalBytes + b.length) {
					if (!noRollingMode && !rollFile()) {
						noRollingMode = true;
						slog.error(
								"araqne log api: other process hold log file more than 10min, turn logger [{}] to non-rolling mode",
								logger.getFullName());
					}
					totalBytes = b.length;
				} else {
					totalBytes += b.length;
				}

				bos.write(b);
				bos.write('\n');
			} catch (Throwable t) {
				slog.debug("araqne log api: cannot write rolling log file, logger [" + logger.getFullName() + "]", t);
			}
		}
	}
}
