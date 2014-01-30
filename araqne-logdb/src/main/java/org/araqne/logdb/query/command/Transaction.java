package org.araqne.logdb.query.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.TimeSpan;
import org.slf4j.LoggerFactory;

public class Transaction extends QueryCommand {
	private final org.slf4j.Logger logger = LoggerFactory.getLogger(Transaction.class);

	private static class XactContext {
		private Date minTime;
		private Date maxTime;
		private List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>(4);
	}

	private ConcurrentMap<List<Object>, XactContext> transactions = new ConcurrentHashMap<List<Object>, XactContext>();

	public static class TransactionOptions {
		public TimeSpan maxSpan;
		public TimeSpan maxPause;
		public int maxEvents;

		// TODO: use expression
		public String startsWith;
		public String endsWith;
	}

	private TransactionOptions txOptions;
	private List<String> fields;

	public Transaction(TransactionOptions txOptions, List<String> fields) {
		this.txOptions = txOptions;
		this.fields = fields;
	}

	@Override
	public String getName() {
		return "transaction";
	}

	/**
	 * assume that onPush() is called in descending order
	 */
	@Override
	public void onPush(Row row) {
		Object o = row.get("line");
		if (o == null)
			return;

		// generate tx key
		List<Object> keys = new ArrayList<Object>(fields.size());
		for (String field : fields) {
			Object value = row.get(field);
			if (value == null)
				return;

			keys.add(value);
		}

		String line = o.toString();

		XactContext ctx = transactions.get(keys);

		// open tx
		if (txOptions.endsWith != null && line.contains(txOptions.endsWith)) {
			if (ctx != null) {
				// start tx not found if transactions contains key
				// e.g. start end (missing!) end

				Row txRow = new Row();
				txRow.put("_time", ctx.maxTime);
				txRow.put("_tx_keys", keys);
				txRow.put("_tx_orphan", true);
				txRow.put("_tx_rows", ctx.rows);
				pushPipe(txRow);

				if (logger.isDebugEnabled())
					logger.debug("araqne logdb: missing transaction begin [{}]", line);
			} else {
				// open transaction
				ctx = new XactContext();
				ctx.maxTime = (Date) row.get("_time");
				ctx.rows.add(row.map());
				transactions.put(keys, ctx);
			}
		}

		// close tx
		if (txOptions.startsWith != null && line.contains(txOptions.startsWith)) {
			if (ctx != null) {
				// found normal transaction start, pop transaction
				transactions.remove(keys);

				ctx.minTime = (Date) row.get("_time");

				Row txRow = new Row();
				txRow.put("_time", ctx.minTime);
				txRow.put("_tx_keys", keys);
				txRow.put("_tx_rows", ctx.rows);
				txRow.put("_tx_duration", ctx.maxTime.getTime() - ctx.minTime.getTime());
				pushPipe(txRow);

			} else {
				Row txRow = new Row();
				txRow.put("_time", row.get("_time"));
				txRow.put("_tx_keys", keys);
				txRow.put("_tx_orphan", true);
				txRow.put("_tx_rows", Arrays.asList(row.map()));
				pushPipe(txRow);

				// no matching transaction end (not yet completed orphan)
				if (logger.isDebugEnabled())
					logger.debug("araqne logdb: running transaction [{}]", line);
			}
		}
	}

	@Override
	public void onClose(QueryStopReason reason) {
		if (reason == QueryStopReason.End) {
			// flush all orphans

			for (List<Object> keys : transactions.keySet()) {
				XactContext ctx = transactions.get(keys);
				Row txRow = new Row();
				txRow.put("_time", ctx.maxTime);
				txRow.put("_tx_keys", keys);
				txRow.put("_tx_orphan", true);
				txRow.put("_tx_rows", ctx.rows);
				pushPipe(txRow);
			}

			transactions.clear();
		}
	}

}
