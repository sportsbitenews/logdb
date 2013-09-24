package org.araqne.log.api;

import java.util.Date;
import java.util.Map;

/**
 * @since 2.6.0
 * @author xeraph
 *
 */
public class LogParserInput {
	private Date date;
	private String source;
	private Map<String, Object> data;

	public Date getDate() {
		return date;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public Map<String, Object> getData() {
		return data;
	}

	public void setData(Map<String, Object> data) {
		this.data = data;
	}
}
