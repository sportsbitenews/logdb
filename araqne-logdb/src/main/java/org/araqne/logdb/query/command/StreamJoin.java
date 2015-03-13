package org.araqne.logdb.query.command;

import java.io.IOException;
import java.util.Map;

import org.araqne.cron.TickService;
import org.araqne.logdb.ByteBufferResult.ByteBufferResultSet;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryResultSet;
import org.araqne.logdb.QueryService;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.QueryTask;
import org.araqne.logdb.Row;
import org.araqne.logdb.impl.QueryHelper;
import org.araqne.logdb.query.command.Join.JoinType;
import org.araqne.logdb.query.command.Sort.SortField;
import org.araqne.storage.api.RCDirectBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamJoin extends QueryCommand {
	private final Logger logger = LoggerFactory.getLogger(StreamJoin.class);
	private final JoinType joinType;
	private final SortField[] sortFields;
	private final Query subQuery;
	private final HashJoiner hashJoiner;

	private SubQueryTask subQueryTask = new SubQueryTask();
	long lastFlushedTime;

	QueryService queryService;

	private RCDirectBufferManager rcDirectBufferManager;
	
	public StreamJoin(JoinType joinType, SortField[] sortFields, Query subQuery, TickService tickService, QueryService queryService, RCDirectBufferManager rcDirectBufferManager) {
		this.joinType = joinType;
		this.sortFields = sortFields;
		this.subQuery = subQuery;
		this.queryService = queryService;

		this.hashJoiner = new HashJoiner(joinType, sortFields);

		lastFlushedTime = System.currentTimeMillis();

		logger.debug("araqne logdb: join subquery created [{}:{}]", subQuery.getId(), subQuery.getQueryString());

		QueryHelper.setJoinAndUnionDependencies(subQuery.getCommands());

		subQueryTask.run();
	}

	@Override
	public String getName() {
		return "Stream Join";
	}

	@Override
	public void onStart() {
		subQuery.preRun();
	}

	@Override
	public void onClose(QueryStopReason reason) {
		if (reason == QueryStopReason.PartialFetch || reason == QueryStopReason.End) {
			synchronized (hashJoiner) {
				hashJoiner.close();
			}
		} else {
			try {
				synchronized (hashJoiner) {
					hashJoiner.cancel();
				}
			} catch (Throwable t) {
				logger.error("araqne logdb: can not cancel sortMergeJoiner", t);
			}
		}

		try {
			subQuery.stop(reason);
		} catch (Throwable t) {
			logger.error("araqne logdb: cannot stop subquery [" + subQuery.getQueryString() + "]", t);
		} finally {
			subQuery.purge();
		}
	}

	@Override
	public void onPush(Row m) {
		try {
			synchronized (hashJoiner) {
				Map<String, Object> result = hashJoiner.probe(m);
				if(result != null) {
					result.putAll(m.map());
					pushPipe(new Row(result));
				}
				
			}
		} catch (Throwable t) {
			logger.error("araqne logdb: cannot setR on sortMergeJoiner[" + m.toString() + "]", t);
		}
	}

	private class SubQueryTask extends QueryTask {
		@Override
		public void run() {
			logger.debug("logpresso query: StreamJoin's SubQueryTask run [{}]", subQuery.getQueryString());

			QueryResultSet rs = null;
			try {
				subQuery.preRun();
				subQuery.run();
				do {
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
					}
				} while (!subQuery.isFinished());
				
		
				subQuery.postRun();
				rs = subQuery.getResultSet();
				ByteBufferResultSet byteBufferRs = (ByteBufferResultSet) rs;

				// logger.debug("logpresso query: StreamJoin fetch subquery result of query [{}:{}]",
				// query.getId(), query.getQueryString());
				synchronized (hashJoiner) {
					hashJoiner.build(byteBufferRs);
				}
			} catch (IOException e) {
				logger.error("logpresso query: cannot get subquery result of query " + query.getId(), e);
			} finally {
				if (rs != null) {
					rs.close();
				}
			}
		}
	}

}
