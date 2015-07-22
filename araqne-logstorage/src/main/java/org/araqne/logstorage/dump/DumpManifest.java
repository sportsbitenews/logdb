package org.araqne.logstorage.dump;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.msgbus.Marshalable;
import org.json.JSONConverter;
import org.json.JSONException;
import org.json.JSONObject;

public class DumpManifest implements Marshalable {

	private int version;
	private String driverType;
	private Map<String, Integer> tables = new HashMap<String, Integer>();
	private List<DumpTabletEntry> entries = new ArrayList<DumpTabletEntry>();

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public String getDriverType() {
		return driverType;
	}

	public void setDriverType(String driverType) {
		this.driverType = driverType;
	}

	public Map<String, Integer> getTables() {
		return tables;
	}

	public void setTables(Map<String, Integer> tables) {
		this.tables = tables;
	}

	public List<DumpTabletEntry> getEntries() {
		return entries;
	}

	public void setEntries(List<DumpTabletEntry> entries) {
		this.entries = entries;
	}

	@SuppressWarnings("unchecked")
	public static DumpManifest parseJSON(InputStream is) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		byte[] b = new byte[8096];
		while (true) {
			int len = is.read(b);
			if (len < 0)
				break;

			bos.write(b, 0, len);
		}

		Map<String, Object> json = null;
		try {
			json = JSONConverter.parse(new JSONObject(new String(bos.toByteArray(), "utf-8")));
		} catch (JSONException e) {
		}

		List<DumpTabletEntry> entries = new ArrayList<DumpTabletEntry>();
		List<Object> l = (List<Object>) json.get("entries");
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		for (Object o : l) {
			Map<String, Object> d = (Map<String, Object>) o;
			DumpTabletEntry entry = new DumpTabletEntry();
			entry.setTableName((String) d.get("table"));
			entry.setDay(df.parse((String) d.get("day"), new ParsePosition(0)));
			entry.setCount(Long.parseLong(d.get("count").toString()));
			entries.add(entry);
		}

		DumpManifest m = new DumpManifest();
		m.setVersion((Integer) json.get("version"));
		m.setDriverType((String) json.get("driver_type"));
		m.setTables((Map<String, Integer>) json.get("tables"));
		m.setEntries(entries);

		return m;
	}

	public String toJSON() {
		try {
			return JSONConverter.jsonize(marshal());
		} catch (JSONException e) {
			return null;
		}
	}

	@Override
	public Map<String, Object> marshal() {
		List<Object> l = new ArrayList<Object>();
		for (DumpTabletEntry task : entries) {
			l.add(task.marshal());
		}

		Map<String, Object> m = new HashMap<String, Object>();
		m.put("version", 1);
		m.put("driver", driverType);
		m.put("tables", tables);
		m.put("entries", l);
		return m;
	}
}
