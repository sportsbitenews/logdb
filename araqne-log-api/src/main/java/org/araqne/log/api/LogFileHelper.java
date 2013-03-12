package org.araqne.log.api;

import java.io.Closeable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogFileHelper implements Closeable {
	public static final Pattern fileNamePattern = Pattern.compile("E_[0-9]{10}\\.stmp");
	public static final SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyyMMdd HHmmss");

//	private static SimpleDateFormat fileDateFormat = new SimpleDateFormat("yyyyMMddHH");
	private String filePathFormat;
	private ArrayList<SimpleDateFormat> placeHolderFormats = new ArrayList<SimpleDateFormat>();
	private File writerFile = null;
	private PrintWriter writer = null;
	private static Pattern placeHolderPattern = Pattern.compile("%%d\\{([^\\}]*)\\}");
	private int fileDurationHour;

	public LogFileHelper(String format, int fileDurationHour) throws IllegalArgumentException {
		this.fileDurationHour = fileDurationHour;
		parse(format);
	}

	private void parse(String format) {
		// sanitize input
		format = Pattern.compile("%(.)").matcher(format).replaceAll("%%$1");
		Matcher matcher = placeHolderPattern.matcher(format);

		StringBuilder builder = new StringBuilder();
		int start = 0;
		int lastEnd = 0;
		while (matcher.find(start)) {
			builder.append(format.substring(start, matcher.start()));
			SimpleDateFormat f = new SimpleDateFormat(matcher.group(1));
			placeHolderFormats.add(f);
			builder.append("%s");
			lastEnd = matcher.end();
			start = lastEnd;
		}
		builder.append(format.substring(lastEnd));
		// sanitize input
		filePathFormat = builder.toString();
	}

	public PrintWriter getWriter(Date date) throws FileNotFoundException {
		File curr = getCurrentFile(date);
		if (!curr.equals(writerFile)) {
			writerFile = curr;
			openNewWriter(writerFile);
		}

		return writer;
	}

	private void openNewWriter(File f) throws FileNotFoundException {
		if (writer != null) {
			System.out.println("file closed\n");
			writer.close();
		}

		writer = new PrintWriter(new FileOutputStream(f));
	}

	@Override
	public void close() throws IOException {
		if (writer != null) {
			System.out.println("file closed\n");

			writer.close();
		}
	}

	public File getCurrentFile(Date date) {
		Date floored = floored(date, fileDurationHour);
		
		List<String> dateStrs = new ArrayList<String>();
		for (SimpleDateFormat s : placeHolderFormats) {
			dateStrs.add(s.format(floored));
		}
		return new File(String.format(filePathFormat, dateStrs.toArray()));
	}
	
	private Date floored(Date d, int durationHour) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(d);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.set(Calendar.HOUR, cal.get(Calendar.HOUR) / durationHour * durationHour);
		return cal.getTime();
	}

	public static void main(String[] args) {
		LogFileHelper logFileHelper = new LogFileHelper("/var/log/some_app/%d{yyyy-MM-dd}/some_prefix-%d{H}.log", 2);
		System.out.println(logFileHelper.getCurrentFile(new Date()).getAbsolutePath());
		
		Calendar cal = Calendar.getInstance();
		cal.set(2013, 2, 2, 20, 14);
		Date startTime = cal.getTime();
		cal.add(Calendar.HOUR, 12);
		Date current = cal.getTime();

		System.out.println(Arrays.toString(logFileHelper.listFilesFrom(startTime, current, false)));
	}
	
	public static void logicTest(String[] args) {
		// memo: (?:[^;]+;){3}([^;]+);
		String format = "/var/log/some_app/%d{yyyy-MM-dd}/some_prefix-%d{H}.log";

		format = Pattern.compile("%(.)").matcher(format).replaceAll("%%$1");
		
		System.out.println(format);

		Pattern placeHolderPattern = Pattern.compile("%%d\\{([^\\}]*)\\}");

		Matcher matcher = placeHolderPattern.matcher(format);

		ArrayList<SimpleDateFormat> phf = new ArrayList<SimpleDateFormat>();
		StringBuilder builder = new StringBuilder();
		int start = 0;
		int lastEnd = 0;
		while (matcher.find(start)) {
			builder.append(format.substring(start, matcher.start()));
			phf.add(new SimpleDateFormat(matcher.group(1)));
			builder.append("%s");
			lastEnd = matcher.end();
			start = lastEnd;
		}
		builder.append(format.substring(lastEnd));

		String formatStr = builder.toString();
		System.out.println(formatStr);
		for (SimpleDateFormat f : phf) {
			System.out.println(f.toPattern());
		}

		List<String> dateStrs = new ArrayList<String>();

		Calendar cal = Calendar.getInstance();
		cal.set(2013, 2, 2, 20, 14);

		// listFilesFrom
		Date d = cal.getTime();
		Date current = new Date();
		
		int cnt = 0;
		while (d.before(current) && cnt++ < 5) {
			dateStrs.clear();
			for (SimpleDateFormat s : phf) {
				dateStrs.add(s.format(d));
			}
			System.out.println(String.format(formatStr, dateStrs.toArray()));
			d.setTime(d.getTime() + 1000 * 60 * 60 * 2);
		}
		
		
	}

	public File[] listFilesFrom(Date startTime, Date current, boolean onlyExists) {
		ArrayList<File> files = new ArrayList<File>();
		Date d = floored(startTime, fileDurationHour);
		while (d.before(current)) {
			ArrayList<String> dateStrs = new ArrayList<String>();
			for (SimpleDateFormat s : placeHolderFormats) {
				dateStrs.add(s.format(d));
			}
			File f = new File(String.format(filePathFormat, dateStrs.toArray()));
			if (!onlyExists || f.exists())
				files.add(f);
			d.setTime(d.getTime() + fileDurationHour * 60 * 60 * 1000);
		}
		return files.toArray(new File[files.size()]);
	}

	private static class LogFileFilter implements FileFilter {
		private final String lastFile;
		private final long lastPos;

		public LogFileFilter(File lastFile, long lastPos) {
			this.lastFile = lastFile == null ? null : lastFile.getName();
			this.lastPos = lastPos;
		}

		@Override
		public boolean accept(File pathname) {
			if (LogFileHelper.fileNamePattern.matcher(pathname.getName()).matches()) {
				if (lastFile == null)
					return true;

				String name = pathname.getName();
				if (name.compareTo(lastFile) > 0)
					return true;
				else if (name.compareTo(lastFile) == 0 && pathname.length() > lastPos)
					return true;
				else
					return false;
			} else
				return false;
		}
	}

}
