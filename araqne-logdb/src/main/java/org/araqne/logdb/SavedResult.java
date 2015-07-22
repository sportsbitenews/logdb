/*
 * Copyright 2013 Eediom Inc.
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
package org.araqne.logdb;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.araqne.api.FieldOption;
import org.araqne.confdb.CollectionName;
import org.araqne.msgbus.Marshalable;

@CollectionName("saved_results")
public class SavedResult implements Marshalable, Comparable<SavedResult> {
	@FieldOption(nullable = false)
	private String guid = UUID.randomUUID().toString();

	@FieldOption(nullable = false)
	private String title;

	@FieldOption(nullable = false)
	private String owner;

	@FieldOption(nullable = false)
	private String queryString;

	@FieldOption(name = "storage", nullable = false)
	private String storageName = "v2";

	@FieldOption(skip = true)
	private String indexPath;

	@FieldOption(skip = true)
	private String dataPath;

	@FieldOption(nullable = false)
	private long fileSize;

	@FieldOption(nullable = false)
	private long rowCount;

	@FieldOption(nullable = false)
	private Date created = new Date();

	/**
	 * @since 2.4.40
	 */
	private List<String> fieldNames;

	public String getGuid() {
		return guid;
	}

	public void setGuid(String guid) {
		this.guid = guid;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public String getStorageName() {
		return storageName;
	}

	public void setStorageName(String type) {
		this.storageName = type;
	}

	public String getIndexPath() {
		return indexPath;
	}

	public void setIndexPath(String indexPath) {
		this.indexPath = indexPath;
	}

	public String getDataPath() {
		return dataPath;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public long getRowCount() {
		return rowCount;
	}

	public void setRowCount(long rowCount) {
		this.rowCount = rowCount;
	}

	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

	public List<String> getFieldNames() {
		return fieldNames;
	}

	public void setFieldNames(List<String> fieldNames) {
		this.fieldNames = fieldNames;
	}

	@Override
	public String toString() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return "guid=" + guid + ", title=" + title + ", owner=" + owner + ", query=" + queryString + ", storage=" + storageName
				+ ", index_path=" + indexPath + ", data_path=" + dataPath + ", file_size=" + fileSize + ", rows=" + rowCount
				+ ", created=" + df.format(created);
	}

	public static SavedResult parse(Map<String, Object> m) {
		SavedResult result = new SavedResult();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		result.setGuid((String) m.get("guid"));
		result.setTitle((String) m.get("title"));
		result.setOwner((String) m.get("owner"));
		result.setQueryString((String) m.get("query_string"));
		result.setStorageName((String) m.get("storage_name"));
		result.setIndexPath((String) m.get("index_path"));
		result.setDataPath((String) m.get("data_path"));
		result.setFileSize(Long.parseLong(m.get("file_size").toString()));
		result.setRowCount(Long.parseLong(m.get("row_count").toString()));
		try {
			result.setCreated(df.parse((String) m.get("created")));
		} catch (ParseException e) {
		}

		return result;
	}

	@Override
	public Map<String, Object> marshal() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		Map<String, Object> m = new HashMap<String, Object>();
		m.put("guid", guid);
		m.put("title", title);
		m.put("owner", owner);
		m.put("query_string", queryString);
		m.put("storage_name", storageName);
		m.put("index_path", indexPath);
		m.put("data_path", dataPath);
		m.put("file_size", fileSize);
		m.put("row_count", rowCount);
		m.put("created", df.format(created));

		return m;
	}

	@Override
	public int compareTo(SavedResult o) {
		if (o == null)
			return -1;

		return o.created.compareTo(created);
	}
}
