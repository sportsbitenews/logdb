package org.araqne.logdb;

import java.util.Date;

public class QueryResultConfig {
	private Query query;
	private String tag;
	private Date created = new Date();

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}
}
