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
package org.araqne.logdb.query.command;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.log.api.LogParserRegistry;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.impl.Strings;
import org.araqne.logdb.query.engine.QueryTask;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Table extends QueryCommand {
	private final Logger logger = LoggerFactory.getLogger(Table.class);
	private AccountService accountService;
	private LogStorage storage;
	private LogTableRegistry tableRegistry;
	private LogParserFactoryRegistry parserFactoryRegistry;
	private LogParserRegistry parserRegistry;

	private TableParams params = new TableParams();

	private TableScanTask scanTask;

	@Override
	public QueryTask getMainTask() {
		return scanTask;
	}

	public Table(TableParams params) {
		this.params = params;
		this.scanTask = new TableScanTask(this, params);
	}

	@Override
	public boolean isDriver() {
		return true;
	}

	public List<String> getTableNames() {
		return params.tableNames;
	}

	public void setTableNames(List<String> tableNames) {
		params.tableNames = tableNames;
	}

	public AccountService getAccountService() {
		return accountService;
	}

	public void setAccountService(AccountService accountService) {
		this.accountService = accountService;
	}

	public LogStorage getStorage() {
		return storage;
	}

	public void setStorage(LogStorage storage) {
		this.storage = storage;
	}

	public LogTableRegistry getTableRegistry() {
		return tableRegistry;
	}

	public void setTableRegistry(LogTableRegistry tableRegistry) {
		this.tableRegistry = tableRegistry;
	}

	public LogParserFactoryRegistry getParserFactoryRegistry() {
		return parserFactoryRegistry;
	}

	public void setParserFactoryRegistry(LogParserFactoryRegistry parserFactoryRegistry) {
		this.parserFactoryRegistry = parserFactoryRegistry;
	}

	public LogParserRegistry getParserRegistry() {
		return parserRegistry;
	}

	public void setParserRegistry(LogParserRegistry parserRegistry) {
		this.parserRegistry = parserRegistry;
	}

	public long getOffset() {
		return params.offset;
	}

	public void setOffset(long offset) {
		params.offset = offset;
	}

	public long getLimit() {
		return params.limit;
	}

	public void setLimit(long limit) {
		params.limit = limit;
	}

	public Date getFrom() {
		return params.from;
	}

	public Date getTo() {
		return params.to;
	}

	@Override
	public void onClose(QueryStopReason reason) {
		if (logger.isDebugEnabled())
			logger.debug("araqne logdb: stopping table scan, query [{}] reason [{}]", getQuery().getId(), reason);
		scanTask.stop();
	}

	public static class TableParams {
		private List<String> tableNames;
		private long offset;
		private long limit;
		private Date from;
		private Date to;
		private String parserName;

		public List<String> getTableNames() {
			return tableNames;
		}

		public void setTableNames(List<String> tableNames) {
			this.tableNames = tableNames;
		}

		public long getOffset() {
			return offset;
		}

		public void setOffset(long offset) {
			this.offset = offset;
		}

		public long getLimit() {
			return limit;
		}

		public void setLimit(long limit) {
			this.limit = limit;
		}

		public Date getFrom() {
			return from;
		}

		public void setFrom(Date from) {
			this.from = from;
		}

		public Date getTo() {
			return to;
		}

		public void setTo(Date to) {
			this.to = to;
		}

		public String getParserName() {
			return parserName;
		}

		public void setParserName(String parserName) {
			this.parserName = parserName;
		}

	}

	@Override
	public String toString() {
		String s = "table";

		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		if (params.getFrom() != null)
			s += " from=" + df.format(params.getFrom());

		if (params.getTo() != null)
			s += " to=" + df.format(params.getTo());

		if (params.getOffset() > 0)
			s += " offset=" + params.getOffset();

		if (params.getLimit() > 0)
			s += " limit=" + params.getLimit();

		return s + " " + Strings.join(params.getTableNames(), ", ");
	}
}
