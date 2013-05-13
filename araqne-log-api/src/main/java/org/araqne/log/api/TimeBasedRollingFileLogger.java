package org.araqne.log.api;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class TimeBasedRollingFileLogger extends AbstractLogger {

	private static int closeWaitDuration = 3000;

	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TimeBasedRollingFileLogger.class);

	public static final String optNameFilePathFormat = "filepath_format";
	public static final String optNameCloseWaitMillisec = "close_wait_millisec";
	public static final String optNameDateFormat = "date_format";
	public static final String optNameStartTime = "start_time";
	public static final String optNameLastLogPath = "lastlog_path";
	public static final String optNameFileDuration = "file_duration";

	private final DateFormat startTimeDateFormat = new SimpleDateFormat("yyyyMMdd HHmm");

	private LogFileHelper logFileHelper;

	File currentFile;

	private LastLog lastLog;

	private Date startTime;

	private long lastModified = 0;

	private File lastLogPath;

	private DateParser dateParser = null;

	public TimeBasedRollingFileLogger(LoggerSpecification spec, LoggerFactory loggerFactory) {
		super(spec.getName(), spec.getDescription(), loggerFactory, spec.getConfig());

		if (spec.getConfig().containsKey(optNameStartTime))
			try {
				startTime = startTimeDateFormat.parse((String) spec.getConfig().get(optNameCloseWaitMillisec));
			} catch (ParseException e) {
				throw new IllegalArgumentException("cannot parse start time");
			}
		else
			startTime = new Date();

		if (spec.getConfig().containsKey(optNameCloseWaitMillisec))
			closeWaitDuration = Integer.parseInt((String) spec.getConfig().get(optNameCloseWaitMillisec));
		else
			closeWaitDuration = 3000;

		int fileDuration = Integer.parseInt((String) spec.getConfig().get(optNameFileDuration)) * 1000;
		logFileHelper = new LogFileHelper((String) spec.getConfig().get(optNameFilePathFormat), fileDuration);

		String dateFormat = (String) spec.getConfig().get(optNameDateFormat);
		if (dateFormat == null)
			dateFormat = "yyyy-MM-dd HH:mm:ss";

		dateParser = new DefaultDateParser(dateFormat);

		lastLogPath = new File((String) spec.getConfig().get(optNameLastLogPath));
		if (!lastLogPath.getParentFile().exists())
			throw new IllegalArgumentException("Parent dir of lastLogPath does not exists.");

	}

	@Override
	protected void runOnce() {
		if (lastLog == null) {
			initAndLoadOldLogs();
		} else {
			loadIncrementally();
		}

	}

	private void initAndLoadOldLogs() {
		lastLog = loadLastLog();
		File[] files = logFileHelper.listFilesFrom(startTime, new Date(), true);

		if (files.length == 0)
			return;

		int startIdx = 0;
		// read remaining
		if (files[0].equals(lastLog.getFile())) {
			startIdx = 1;
			loadFile(files[0], lastLog.getPos());
		}

		for (int i = startIdx; i < files.length; ++i) {
			loadFile(files[i]);
		}
	}

	private static class LastLog {
		private File file;
		private long pos;

		public LastLog(File file, long lastPos) {
			super();
			this.file = file;
			this.pos = lastPos;
		}

		public File getFile() {
			return file;
		}

		public long getPos() {
			return pos;
		}
	}

	private void saveLastLog(File file, long lastPos) {
		PrintWriter writer = null;
		try {
			lastLog = new LastLog(file, lastPos);
			logger.trace("lastLog: {}, {}", lastLog.getFile().getAbsolutePath(), lastLog.getPos());
			File newLostLog = new File(currentFile.getParentFile(), ".logpresso-lastlog.new");
			writer = new PrintWriter(newLostLog);
			writer.println(String.format("%s %d", file.getName(), lastPos));
			writer.close();
			writer = null;

			File dest = new File(currentFile.getParentFile(), ".logpresso-lastlog");
			File old = new File(currentFile.getParentFile(), ".logpresso-lastlog.old");
			if (dest.exists() && !dest.renameTo(old))
				throw new IllegalStateException("failed to save logpresso-lastlog in "
						+ currentFile.getParentFile().getAbsolutePath());
			else
				old.delete();

			if (!newLostLog.renameTo(dest))
				throw new IllegalStateException("failed to save logpresso-lastlog in "
						+ currentFile.getParentFile().getAbsolutePath());
			else
				old.delete();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			if (writer != null)
				writer.close();
		}

	}

	@Override
	protected void onStop() {
		super.onStop();
		lastLog = null;
	}

	private long loadFile(File file, Long startPos) {
		FileInputStream fis = null;
		try {
			long nextPos = file.length();
			fis = new FileInputStream(file);
			fis.skip(startPos);
			BufferedInputStream bis = new BufferedInputStream(fis);
			long remaining = nextPos - startPos;
			ByteBuffer bb = ByteBuffer.allocate(81920);
			while (remaining > 0) {
				int readReq = bb.remaining();
				if (remaining < bb.remaining())
					readReq = (int) remaining;
				int read = bis.read(bb.array(), bb.position(), readReq);
				bb.position(bb.position() + read);
				bb.flip();
				remaining -= read;
				int incompletedLineLength = findIncompletedLine(bb.array(), bb.limit());
				Scanner s = new Scanner(new ByteArrayInputStream(bb.array(), 0, bb.limit() - incompletedLineLength));
				s.useDelimiter("[\r\n]");
				while (s.hasNextLine()) {
					String line = s.nextLine();
					writeLog(line);
				}
				bb.position(bb.limit() - incompletedLineLength);
				bb.compact();
			}

			saveLastLog(file, nextPos);

			return nextPos;
		} catch (FileNotFoundException e) {
			// skip
		} catch (IOException e) {
			logger.error("error while reading file", e);
		} finally {
			ensureClose(fis);
		}

		saveLastLog(file, startPos);
		return startPos;
	}

	private int findIncompletedLine(byte[] b, int len) {
		for (int i = len; i > 1; --i) {
			if (b[i - 1] == '\n')
				return len - i;
		}
		return len;
	}

	private void writeLog(String line) {
		Map<String, Object> params = new HashMap<String, Object>();
		Date date = extractDate(line);
		if (date == null) {
			logger.warn("skipped log: " + line);
			return;
		}

		params.put("date", date);
		params.put("line", line);

		Log log = new SimpleLog(date, getFullName(), params);
		write(log);
	}

	private Date extractDate(String line) {
		Date parse = dateParser.parse(line);
		if (parse != null)
			return parse;
		else
			return new Date();
	}

	private void ensureClose(Closeable fis) {
		try {
			if (fis != null)
				fis.close();
		} catch (Exception e) {
		}
	}

	private long loadFile(File file) {
		return loadFile(file, 0L);
	}

	private LastLog loadLastLog() {
		Scanner s = null;
		try {
			s = new Scanner(lastLogPath);
			String line = s.useDelimiter("[\\r\\n]").next();

			String[] split = line.split(" ");
			if (split.length < 2)
				throw new IllegalStateException(".logpresso-lastlog file broken");
			String filename = split[0];
			long pos = Long.parseLong(split[1]);

			return new LastLog(new File(currentFile.getParentFile(), filename), (Long) pos);
		} catch (FileNotFoundException e) {
			return new LastLog(null, 0L);
		} finally {
			if (s != null)
				s.close();
		}
	}

	private void loadIncrementally() {
		Date currentTime = new Date();
		File currentFile = logFileHelper.getCurrentFile(currentTime);
		if (currentFile.equals(lastLog.getFile())) {
			loadFile(lastLog.getFile(), lastLog.getPos());
		} else {
			logger.trace("logger on hour edge");
			// on hour edge
			if (lastLog.getFile() == null) {
				loadFile(currentFile, 0L);
				return;
			}

			if (lastLog.getFile().length() != lastLog.getPos()) {
				logger.trace("detected remaining data on last file");
				loadFile(lastLog.getFile(), lastLog.getPos());
				lastModified = currentTime.getTime();
			} else {
				// load last file until being able to ensure being closed
				if (lastModified != 0 && currentTime.getTime() - lastModified < closeWaitDuration) {
					logger.trace("waiting for last file being closed");
					// wait for being closed
				} else {
					logger.trace("starting to read current file");
					lastModified = 0;
					loadFile(currentFile, 0L);
				}
			}
		}
	}
}
