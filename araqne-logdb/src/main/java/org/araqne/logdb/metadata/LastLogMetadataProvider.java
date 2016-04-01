package org.araqne.logdb.metadata;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.FieldOrdering;
import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.MetadataCallback;
import org.araqne.logdb.MetadataProvider;
import org.araqne.logdb.MetadataService;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;

@Component(name = "logdb-last-log-metadata")
public class LastLogMetadataProvider implements MetadataProvider, FieldOrdering {
	@Requires
	private LogTableRegistry tableRegistry;

	@Requires
	private AccountService accountService;

	@Requires
	private LogStorage storage;

	@Requires
	private FunctionRegistry functionRegistry;

	@Requires
	private MetadataService metadataService;

	@Validate
	public void start() {
		metadataService.addProvider(this);
	}

	@Invalidate
	public void stop() {
		if (metadataService != null)
			metadataService.removeProvider(this);
	}

	@Override
	public List<String> getFieldOrder() {
		return Arrays.asList("table_name", "date", "last_log");
	}

	@Override
	public String getType() {
		return "lastlog";
	}

	@Override
	public void verify(QueryContext context, String queryString) {
		MetadataQueryStringParser.getTableNames(context, tableRegistry, accountService, functionRegistry, queryString);
	}

	@Override
	public void query(QueryContext context, String queryString, MetadataCallback callback) {
		TableScanOption opt = MetadataQueryStringParser.getTableNames(context, tableRegistry, accountService, functionRegistry,
				queryString);
		List<String> targetTables = opt.getTableNames();

		for (String tableName : tableRegistry.getTableNames()) {
			if (!targetTables.contains(tableName))
				continue;

			Map<String, Object> m = new HashMap<String, Object>();
			m.put("table_name", tableName);
			Date lastDate = getLastDate(tableName);
			if (lastDate != null) {
				Iterator<Log> it = storage.getLogs(tableName, lastDate, null, 1).iterator();
				if (it.hasNext()) {
					Log log = it.next();
					m.put("date", log.getDate());
					m.put("data", log.getData());
				}
			} else {
				m.put("date", null);
				m.put("data", null);
			}

			callback.onPush(new Row(m));
		}
	}

	private Date getLastDate(String tableName) {
		Iterator<Date> it = storage.getLogDates(tableName).iterator();
		Date lastDay = null;
		if (it.hasNext())
			lastDay = it.next();

		return lastDay;
	}

}
