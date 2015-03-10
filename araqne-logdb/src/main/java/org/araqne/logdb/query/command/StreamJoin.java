package org.araqne.logdb.query.command;

import java.io.IOException;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.SystemMenuBar;

import org.araqne.cron.AbstractTickTimer;
import org.araqne.cron.TickService;
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
import org.araqne.logdb.query.command.SortMergeJoiner;
import org.araqne.logdb.query.engine.QueryTaskRunner;
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
	
	public StreamJoin(JoinType joinType, SortField[] sortFields, Query subQuery, TickService tickService, QueryService queryService) {
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
				sortMergeJoiner.close();
			}
		} else {
			try {
				synchronized (sortMergeJoiner) {
					sortMergeJoiner.cancel();
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
				hashJoiner.probe(m);
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
				Query query = queryService.createQuery(subQuery.getContext().getSession(), subQuery.getQueryString());
				queryService.startQuery(query.getId());
				do {
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
					}
				} while (!query.isFinished());
					
				rs = query.getResultSet();

				// logger.debug("logpresso query: StreamJoin fetch subquery result of query [{}:{}]",
				// query.getId(), query.getQueryString());
				synchronized (hashJoiner) {
					hashJoiner.build(rs);
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
