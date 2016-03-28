package org.araqne.logstorage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogVectors {
	public String tableName;
	public boolean selectedInUse;
	public int size;
	public int[] selected;
	public Date[] dateVector;
	public long[] idVector;
	public Map<String, LogVector> data;

	public List<Log> toLogList() {
		Log[] logs = new Log[size];
		Map<String, Object>[] dataList = new Map[size];

		if (selectedInUse) {
			for (int i = 0; i < size; i++) {
				int p = selected[i];
				dataList[i] = new HashMap<String, Object>();
				logs[i] = new Log(tableName, dateVector[p], idVector[p], dataList[i]);
			}
		} else {
			for (int i = 0; i < size; i++) {
				dataList[i] = new HashMap<String, Object>();
				logs[i] = new Log(tableName, dateVector[i], idVector[i], dataList[i]);
			}
		}

		if (selectedInUse) {
			for (String key : data.keySet()) {
				LogVector vec = data.get(key);
				Object[] array = vec.getArray(); 
				for (int i = 0; i < size; i++) {
					int p = selected[i];
					Object val = array[p];
					if (val != null)
						dataList[i].put(key, val);
				}
			}
		} else {
			for (String key : data.keySet()) {
				LogVector vec = data.get(key);
				Object[] array = vec.getArray();
				for (int i = 0; i < size; i++) {
					Object val = array[i];
					if (val != null)
						dataList[i].put(key, val);
				}
			}
		}

		return Arrays.asList(logs);
	}
}
