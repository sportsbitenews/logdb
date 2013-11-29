/*
 * Copyright 2011 Future Systems
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
package org.araqne.logdb.msgbus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.Deflater;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Requires;
import org.araqne.api.PrimitiveConverter;
import org.araqne.codec.Base64;
import org.araqne.codec.FastEncodingRule;
import org.araqne.logdb.QueryService;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryResultCallback;
import org.araqne.logdb.QueryResultSet;
import org.araqne.logdb.QueryStatusCallback;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.QueryTimelineCallback;
import org.araqne.logdb.RunMode;
import org.araqne.logdb.SavedResult;
import org.araqne.logdb.SavedResultManager;
import org.araqne.logdb.impl.QueryHelper;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.msgbus.MsgbusException;
import org.araqne.msgbus.PushApi;
import org.araqne.msgbus.Request;
import org.araqne.msgbus.Response;
import org.araqne.msgbus.Session;
import org.araqne.msgbus.handler.MsgbusMethod;
import org.araqne.msgbus.handler.MsgbusPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "logdb-logquery-msgbus")
@MsgbusPlugin
public class LogQueryPlugin {
	private final Logger logger = LoggerFactory.getLogger(LogQueryPlugin.class.getName());

	@Requires
	private QueryService service;

	@Requires
	private LogTableRegistry tableRegistry;

	@Requires
	private LogStorage storage;

	@Requires
	private PushApi pushApi;

	@Requires
	private SavedResultManager savedResultManager;

	@MsgbusMethod
	public void logs(Request req, Response resp) {
		String tableName = req.getString("table");
		int limit = req.getInteger("limit");
		int offset = 0;
		if (req.has("offset"))
			offset = req.getInteger("offset");

		if (!tableRegistry.exists(tableName))
			throw new MsgbusException("logdb", "table-not-exists");

		Collection<Log> logs = storage.getLogs(tableName, null, null, offset, limit);
		List<Object> serialized = new ArrayList<Object>(limit);
		for (Log log : logs)
			serialized.add(serialize(log));

		resp.put("logs", serialized);
	}

	private Map<String, Object> serialize(Log log) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("table", log.getTableName());
		m.put("id", log.getId());
		m.put("date", log.getDate());
		m.put("data", log.getData());
		return m;
	}

	@MsgbusMethod
	public void queries(Request req, Response resp) {
		org.araqne.logdb.Session dbSession = getDbSession(req);
		List<Object> result = QueryHelper.getQueries(dbSession, service);
		resp.put("queries", result);
	}

	@MsgbusMethod
	public void createQuery(Request req, Response resp) {
		try {
			org.araqne.logdb.Session dbSession = getDbSession(req);
			Query query = service.createQuery(dbSession, req.getString("query"));
			resp.put("id", query.getId());
		} catch (Exception e) {
			logger.error("araqne logdb: cannot create query", e);
			throw new MsgbusException("logdb", e.getMessage());
		}
	}

	@MsgbusMethod
	public void removeQuery(Request req, Response resp) {
		int id = req.getInteger("id", true);
		org.araqne.logdb.Session dbSession = getDbSession(req);
		service.removeQuery(dbSession, id);
	}

	private org.araqne.logdb.Session getDbSession(Request req) {
		return getDbSession(req.getSession());
	}

	private org.araqne.logdb.Session getDbSession(Session session) {
		return (org.araqne.logdb.Session) session.get("araqne_logdb_session");
	}

	@MsgbusMethod
	public void startQuery(Request req, Response resp) {
		String orgDomain = req.getOrgDomain();
		int id = req.getInteger("id");
		int offset = req.getInteger("offset");
		int limit = req.getInteger("limit");
		Integer timelineLimit = req.getInteger("timeline_limit");

		Query query = service.getQuery(id);

		// validation check
		if (query == null) {
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("query_id", id);
			throw new MsgbusException("logdb", "query not found", params);
		}

		if (query.isStarted())
			throw new MsgbusException("logdb", "already running");

		// set query and timeline callback
		QueryResultCallback qc = new MsgbusLogQueryCallback(orgDomain, offset, limit);
		query.getCallbacks().getResultCallbacks().add(qc);

		QueryStatusCallback qs = new MsgbusStatusCallback(orgDomain);
		query.getCallbacks().getStatusCallbacks().add(qs);

		if (timelineLimit != null) {
			int size = timelineLimit.intValue();
			QueryTimelineCallback tc = new MsgbusTimelineCallback(orgDomain, query, size);
			query.getCallbacks().getTimelineCallbacks().add(tc);
		}

		// start query
		service.startQuery(query.getId());
	}

	@MsgbusMethod
	public void stopQuery(Request req, Response resp) {
		int id = req.getInteger("id", true);
		Query query = service.getQuery(id);
		if (query != null)
			query.stop(QueryStopReason.UserRequest);
		else {
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("query_id", id);
			throw new MsgbusException("logdb", "query-not-found", params);
		}
	}

	@MsgbusMethod
	public void getResult(Request req, Response resp) throws IOException {
		int id = req.getInteger("id", true);
		int offset = req.getInteger("offset", true);
		int limit = req.getInteger("limit", true);
		Boolean binaryEncode = req.getBoolean("binary_encode");

		Map<String, Object> m = QueryHelper.getResultData(service, id, offset, limit);
		if (m == null)
			return;

		FastEncodingRule enc = new FastEncodingRule();
		if (binaryEncode != null && binaryEncode) {
			ByteBuffer binary = enc.encode(m);
			int uncompressedSize = binary.array().length;
			byte[] b = compress(binary.array());
			resp.put("binary", new String(Base64.encode(b)));
			resp.put("uncompressed_size", uncompressedSize);
		} else
			resp.putAll(m);
	}

	private byte[] compress(byte[] b) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream(b.length);
		Deflater c = new Deflater();
		try {
			c.reset();
			c.setInput(b);
			c.finish();
			byte[] compressed = new byte[b.length];

			while (true) {
				int compressedSize = c.deflate(compressed);
				if (compressedSize == 0)
					break;
				bos.write(compressed, 0, compressedSize);
			}

			return bos.toByteArray();
		} finally {
			c.end();
		}
	}

	/**
	 * @since 0.17.0
	 */
	@MsgbusMethod
	public void setRunMode(Request req, Response resp) {
		int id = req.getInteger("id", true);
		boolean background = req.getBoolean("background", true);

		Query query = service.getQuery(id);
		if (query == null)
			throw new MsgbusException("logdb", "query-not-found");

		org.araqne.logdb.Session dbSession = getDbSession(req);

		if (!query.isAccessible(dbSession))
			throw new MsgbusException("logdb", "no-permission");

		query.setRunMode(background ? RunMode.BACKGROUND : RunMode.FOREGROUND, new QueryContext(dbSession));
	}

	/**
	 * @since 1.4.0
	 */
	@MsgbusMethod
	public void queryStatus(Request req, Response resp) {
		int id = req.getInteger("id", true);
		org.araqne.logdb.Session dbSession = getDbSession(req);

		Query query = service.getQuery(dbSession, id);
		if (query == null)
			throw new MsgbusException("logdb", "query-not-found");

		resp.putAll(QueryHelper.getQuery(query));
	}

	@MsgbusMethod
	public void getSavedResults(Request req, Response resp) {
		org.araqne.logdb.Session dbSession = getDbSession(req);

		List<SavedResult> l = savedResultManager.getResultList(dbSession.getLoginName());
		resp.put("saved_results", PrimitiveConverter.serialize(l));
	}

	/**
	 * @since 1.6.8
	 */
	@MsgbusMethod
	public void saveResult(Request req, Response resp) {
		String title = req.getString("title", true);
		int queryId = req.getInteger("query_id", true);
		Query query = service.getQuery(queryId);
		if (query == null)
			throw new MsgbusException("logdb", "query-not-found");

		QueryResultSet rs = null;
		try {
			rs = query.getResultSet();
			long total = rs.getIndexPath().length() + rs.getDataPath().length();

			org.araqne.logdb.Session dbSession = getDbSession(req);

			SavedResult sr = new SavedResult();
			sr.setType("v2");
			sr.setOwner(dbSession.getLoginName());
			sr.setQueryString(query.getQueryString());
			sr.setTitle(title);
			sr.setIndexPath(rs.getIndexPath().getAbsolutePath());
			sr.setDataPath(rs.getDataPath().getAbsolutePath());
			sr.setRowCount(rs.size());
			sr.setFileSize(total);

			savedResultManager.saveResult(sr);
			resp.put("guid", sr.getGuid());
		} catch (IOException e) {
			logger.error("araqne logdb: cannot save result of query " + query.getId(), e);
			throw new MsgbusException("logdb", "io-error");
		} finally {
			if (rs != null)
				rs.close();
		}
	}

	/**
	 * @since 1.6.8
	 */
	@MsgbusMethod
	public void deleteResult(Request req, Response resp) {
		org.araqne.logdb.Session dbSession = getDbSession(req);
		String guid = req.getString("guid", true);
		try {
			SavedResult sr = savedResultManager.getResult(guid);
			if (sr == null)
				throw new MsgbusException("logdb", "saved-result-not-found");

			if (!sr.getOwner().equals(dbSession.getLoginName()))
				throw new MsgbusException("logdb", "no-permission");

			savedResultManager.deleteResult(guid);
		} catch (IOException e) {
			throw new MsgbusException("logdb", "io-error");
		}
	}

	@SuppressWarnings("unchecked")
	@MsgbusMethod
	public void deleteResults(Request req, Response resp) {
		org.araqne.logdb.Session dbSession = getDbSession(req);
		List<String> guids = (List<String>) req.get("guids", true);

		try {
			for (String guid : guids) {
				SavedResult sr = savedResultManager.getResult(guid);
				if (sr == null)
					throw new MsgbusException("logdb", "saved-result-not-found");

				if (!sr.getOwner().equals(dbSession.getLoginName()))
					throw new MsgbusException("logdb", "no-permission");

				savedResultManager.deleteResult(guid);
			}
		} catch (IOException e) {
			throw new MsgbusException("logdb", "io-error");
		}
	}

	private class MsgbusStatusCallback implements QueryStatusCallback {
		private String orgDomain;

		private MsgbusStatusCallback(String orgDomain) {
			this.orgDomain = orgDomain;
		}

		@Override
		public void onChange(Query query) {
			if (query.isFinished()) {
				try {
					Map<String, Object> m = new HashMap<String, Object>();
					m.put("id", query.getId());
					m.put("type", "eof");
					m.put("total_count", query.getResultCount());
					pushApi.push(orgDomain, "logdb-query-" + query.getId(), m);
					pushApi.push(orgDomain, "logstorage-query-" + query.getId(), m); // deprecated

					query.getCallbacks().getStatusCallbacks().remove(this);
				} catch (IOException e) {
					logger.error("araqne logdb: msgbus push fail", e);
				}
			} else {
				try {
					String status = null;
					if (!query.isStarted())
						status = "Waiting";
					else
						status = query.isFinished() ? "End" : "Running";

					Map<String, Object> m = new HashMap<String, Object>();
					m.put("id", query.getId());
					m.put("type", "status_change");
					m.put("status", status);
					m.put("count", query.getResultCount());
					pushApi.push(orgDomain, "logdb-query-" + query.getId(), m);
					pushApi.push(orgDomain, "logstorage-query-" + query.getId(), m); // deprecated
				} catch (IOException e) {
					logger.error("araqne logdb: msgbus push fail", e);
				}
			}
		}
	}

	private class MsgbusLogQueryCallback implements QueryResultCallback {
		private int offset;
		private int limit;
		private String orgDomain;
		private boolean pageLoaded;

		private MsgbusLogQueryCallback(String orgDomain, int offset, int limit) {
			this.orgDomain = orgDomain;
			this.offset = offset;
			this.limit = limit;
		}

		@Override
		public int offset() {
			return offset;
		}

		@Override
		public int limit() {
			return limit;
		}

		@Override
		public void onPageLoaded(Query query) {
			try {
				if (pageLoaded)
					return;

				Map<String, Object> m = QueryHelper.getResultData(service, query.getId(), offset, limit);
				m.put("id", query.getId());
				m.put("type", "page_loaded");
				pushApi.push(orgDomain, "logdb-query-" + query.getId(), m);
				pushApi.push(orgDomain, "logstorage-query-" + query.getId(), m); // deprecated

				pageLoaded = true;
			} catch (IOException e) {
				logger.error("araqne logdb: msgbus push fail", e);
			}
		}
	}

	private class MsgbusTimelineCallback extends QueryTimelineCallback {
		private Logger logger = LoggerFactory.getLogger(MsgbusTimelineCallback.class);
		private String orgDomain;
		private Query query;
		private int size;

		private MsgbusTimelineCallback(String orgDomain, Query query) {
			this(orgDomain, query, 10);
		}

		private MsgbusTimelineCallback(String orgDomain, Query query, int size) {
			this.orgDomain = orgDomain;
			this.query = query;
			this.size = size;
		}

		@Override
		public int getSize() {
			return size;
		}

		@Override
		protected void callback(Date beginTime, SpanValue spanValue, int[] values, boolean isEnd) {
			try {
				Map<String, Object> m = new HashMap<String, Object>();
				m.put("id", query.getId());
				m.put("type", isEnd ? "eof" : "periodic");
				m.put("span_field", spanValue.getFieldName());
				m.put("span_amount", spanValue.getAmount());
				m.put("begin", beginTime);
				m.put("values", values);
				pushApi.push(orgDomain, "logdb-query-timeline-" + query.getId(), m);

				m.put("count", query.getResultCount());
				pushApi.push(orgDomain, "logstorage-query-timeline-" + query.getId(), m); // deprecated

				Object[] trace = new Object[] { query.getId(), spanValue.getFieldName(), spanValue.getAmount(), beginTime,
						Arrays.toString(values), query.getResultCount() };
				logger.trace("araqne logdb: timeline callback => "
						+ "{id={}, span_field={}, span_amount={}, begin={}, values={}, count={}}", trace);
			} catch (IOException e) {
				logger.error("araqne logdb: msgbus push fail", e);
			}
		}
	}
}
