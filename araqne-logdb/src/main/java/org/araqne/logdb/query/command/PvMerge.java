package org.araqne.logdb.query.command;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;

public class PvMerge extends QueryCommand {

	private final List<String> columns;

	private Object[] ck = null;
	private HashMap<String, Object> nr = null;

	private final String kcName;
	private final String vcName;

	public PvMerge(List<String> keyColumns, String kcName, String vcName) {
		this.columns = keyColumns;
		this.kcName = kcName;
		this.vcName = vcName;
	}

	@Override
	public void onClose(QueryStopReason reason) {
		pushPipe(new Row(nr));
		super.onClose(reason);
	}

	@Override
	public void onPush(Row row) {
		Object[] nk = new Object[columns.size()];
		for (int i = 0; i < columns.size(); ++i) {
			nk[i] = row.get(columns.get(i));
		}
		
		if (ck != null) {
			if (Arrays.equals(ck, nk)) {
				nr.put(row.get(kcName).toString(), row.get(vcName));
			} else {
				pushPipe(new Row(nr));
				ck = null;
			}
		}

		if (ck == null) {
			ck = nk;
			nr = new HashMap<String, Object>();
			for (String cname : columns) {
				nr.put(cname, row.get(cname));
			}
			nr.put(row.get(kcName).toString(), row.get(vcName));
		}
	}

	@Override
	public String getName() {
		return "pvmerge";
	}

}
