package org.araqne.logdb;

import java.util.Date;
import java.util.UUID;

import org.araqne.api.FieldOption;
import org.araqne.confdb.CollectionName;

@CollectionName("saved_results")
public class SavedResult {
	@FieldOption(nullable = false)
	private String guid = UUID.randomUUID().toString();

	@FieldOption(nullable = false)
	private String title;

	@FieldOption(nullable = false)
	private String owner;

	@FieldOption(nullable = false)
	private String queryString;

	// v2, v3p
	@FieldOption(nullable = false)
	private String type;

	@FieldOption(nullable = false)
	private String indexPath;

	@FieldOption(nullable = false)
	private String dataPath;

	@FieldOption(nullable = false)
	private Date created = new Date();

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

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
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

	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}
}
