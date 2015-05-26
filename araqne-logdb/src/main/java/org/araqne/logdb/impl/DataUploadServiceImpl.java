package org.araqne.logdb.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.codec.Base64;
import org.araqne.log.api.LogPipe;
import org.araqne.log.api.Logger;
import org.araqne.log.api.MultilineLogExtractor;
import org.araqne.logdb.DataUploadService;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogStorage;

@Component(name = "logdb-data-upload")
@Provides
public class DataUploadServiceImpl implements DataUploadService {
	private static final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(DataUploadServiceImpl.class);
	private ConcurrentMap<String, UploadState> states = new ConcurrentHashMap<String, UploadState>();

	@Override
	public void loadTextFile(LogStorage storage, String ticket, boolean last, String data, String datePattern, String dateFormat,
			String dateLocale, String beginRegex, String endRegex, String tableName, String charset) throws IOException {
		// decode base64 data uri
		int p = data.indexOf(",");
		String s = data.substring(p + 1);
		byte[] b = Base64.decode(s);

		// get upload state
		UploadState state = states.get(ticket);
		if (state == null) {
			state = new UploadState(storage, datePattern, dateFormat, dateLocale, beginRegex, endRegex, tableName, charset);
			states.put(ticket, state);
		}

		MultilineLogExtractor extractor = state.extractor;
		AtomicLong lastPosition = new AtomicLong();

		// transfer decoded input
		state.input.write(b);

		byte[] stateByteArray = state.input.toByteArray();
		ByteArrayInputStream is = new ByteArrayInputStream(stateByteArray);
		extractor.extract(is, lastPosition);

		byte[] lastBuffer = stateByteArray;
		ByteArrayOutputStream nextBuffer = new ByteArrayOutputStream(lastBuffer.length);
		int offset = (int) lastPosition.get();
		nextBuffer.write(lastBuffer, offset, lastBuffer.length - offset);
		state.input = nextBuffer;

		if (last) {
			states.remove(ticket);

			if (nextBuffer.size() > 0)
				slog.warn("araqne logdb: upload ticket [{}] closed, non-written bytes [{}]", ticket, nextBuffer.size());
		}
	}

	@Override
	public List<Map<String, Object>> previewTextFile(String data, String datePattern, String dateFormat, String dateLocale,
			String beginRegex, String endRegex, String charset) throws IOException {
		// decode base64 data uri
		int p = data.indexOf(",");
		String s = data.substring(p + 1);
		byte[] b = Base64.decode(s);

		// get preview state
		PreviewState state = new PreviewState(datePattern, dateFormat, dateLocale, beginRegex, endRegex, charset);

		MultilineLogExtractor extractor = state.extractor;
		AtomicLong lastPosition = new AtomicLong();

		// transfer decoded input
		state.input.write(b);

		byte[] stateByteArray = state.input.toByteArray();
		ByteArrayInputStream is = new ByteArrayInputStream(stateByteArray);
		extractor.extract(is, lastPosition);

		return state.previews;
	}

	private static class UploadState {
		public MultilineLogExtractor extractor;
		public ByteArrayOutputStream input = new ByteArrayOutputStream();

		public UploadState(LogStorage storage, String datePattern, String dateFormat, String dateLocale, String beginRegex,
				String endRegex, String tableName, String charset) {
			DummyLogger logger = new DummyLogger();
			UploadPipe pipe = new UploadPipe(storage, tableName);
			this.extractor = newMultilineExtractor(datePattern, dateFormat, dateLocale, beginRegex, endRegex, charset, logger,
					pipe);
		}

		private MultilineLogExtractor newMultilineExtractor(String datePattern, String dateFormat, String dateLocale,
				String beginRegex, String endRegex, String charset, DummyLogger logger, UploadPipe pipe) {

			MultilineLogExtractor extractor = new MultilineLogExtractor(logger, pipe);
			if (datePattern != null)
				extractor.setDateMatcher(Pattern.compile(datePattern).matcher(""));

			if (dateLocale == null)
				dateLocale = "en";

			if (dateFormat != null)
				extractor.setDateFormat(new SimpleDateFormat(dateFormat, new Locale(dateLocale)), null);

			if (beginRegex != null)
				extractor.setBeginMatcher(Pattern.compile(beginRegex).matcher(""));

			if (endRegex != null)
				extractor.setEndMatcher(Pattern.compile(endRegex).matcher(""));

			if (charset == null)
				charset = "utf-8";

			extractor.setCharset(charset);
			return extractor;
		}
	}

	private static class UploadPipe implements LogPipe {
		private LogStorage storage;
		private String tableName;

		private UploadPipe(LogStorage storage, String tableName) {
			this.storage = storage;
			this.tableName = tableName;
		}

		@Override
		public void onLog(Logger logger, org.araqne.log.api.Log log) {
			if (log == null)
				return;

			Log tableLog = new Log(tableName, log.getDate(), log.getParams());
			try {
				storage.write(tableLog);
			} catch (InterruptedException e) {
				slog.warn("storage.write interrupted", e);
			}
		}

		@Override
		public void onLogBatch(Logger logger, org.araqne.log.api.Log[] logs) {
			List<Log> tableLogs = new ArrayList<Log>();

			for (org.araqne.log.api.Log log : logs) {
				if (log == null)
					continue;

				Log tableLog = new Log(tableName, log.getDate(), log.getParams());
				tableLogs.add(tableLog);
			}

			if (!tableLogs.isEmpty())
				try {
					storage.write(tableLogs);
				} catch (InterruptedException e) {
					slog.warn("storage.write interrupted", e);
				}
		}
	}

	private static class PreviewState {
		public MultilineLogExtractor extractor;
		public ByteArrayOutputStream input = new ByteArrayOutputStream();
		public List<Map<String, Object>> previews = new ArrayList<Map<String, Object>>();

		public PreviewState(String datePattern, String dateFormat, String dateLocale, String beginRegex, String endRegex, String charset) {
			DummyLogger logger = new DummyLogger();
			PreviewPipe pipe = new PreviewPipe(previews);
			this.extractor = newMultilineExtractor(datePattern, dateFormat, dateLocale, beginRegex, endRegex, charset, logger,
					pipe);
		}

		private MultilineLogExtractor newMultilineExtractor(String datePattern, String dateFormat, String dateLocale,
				String beginRegex, String endRegex, String charset, DummyLogger logger, PreviewPipe pipe) {

			MultilineLogExtractor extractor = new MultilineLogExtractor(logger, pipe);
			if (datePattern != null)
				extractor.setDateMatcher(Pattern.compile(datePattern).matcher(""));

			if (dateLocale == null)
				dateLocale = "en";

			if (dateFormat != null)
				extractor.setDateFormat(new SimpleDateFormat(dateFormat, new Locale(dateLocale)), null);

			if (beginRegex != null)
				extractor.setBeginMatcher(Pattern.compile(beginRegex).matcher(""));

			if (endRegex != null)
				extractor.setEndMatcher(Pattern.compile(endRegex).matcher(""));

			if (charset == null)
				charset = "utf-8";

			extractor.setCharset(charset);
			return extractor;
		}
	}

	private static class PreviewPipe implements LogPipe {
		private List<Map<String, Object>> previews;

		private PreviewPipe(List<Map<String, Object>> previews) {
			this.previews = previews;
		}

		@Override
		public void onLog(Logger logger, org.araqne.log.api.Log log) {
			if (log == null)
				return;

			Map<String, Object> m = log.getParams();
			m.put("_time", log.getDate());
			previews.add(m);
		}

		@Override
		public void onLogBatch(Logger logger, org.araqne.log.api.Log[] logs) {
			if (logs == null)
				return;

			for (org.araqne.log.api.Log log : logs)
				if (log != null) {
					Map<String, Object> m = log.getParams();
					m.put("_time", log.getDate());
					previews.add(m);
				}
		}
	}

	@Override
	public void abortTextFile(String ticket) {
		if(ticket != null)
			states.remove(ticket);
		
	}

}
