package org.araqne.logstorage;

import java.util.Date;
import java.util.Map;

public class LogVectors {
	public String tableName;
	public boolean selectedInUse;
	public int size;
	public int[] selected;
	public Date[] dateVector;
	public long[] idVector;
	public Map<String, Object> data;
}
