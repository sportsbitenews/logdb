/**
 * Copyright 2014 Eediom Inc.
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
package org.araqne.logdb.client;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 스트림 쿼리의 설정과 상태를 표현합니다.
 * 
 * @since 0.9.5
 * @author xeraph@eediom.com
 * 
 */
public class StreamQueryInfo {
	private String name;
	private String description;

	private int interval;
	private String queryString;

	// logger, table, or stream
	private String sourceType;

	private List<String> sources = new ArrayList<String>();

	private String owner;
	private boolean enabled;
	private Date created = new Date();
	private Date modified = new Date();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public int getInterval() {
		return interval;
	}

	public void setInterval(int interval) {
		this.interval = interval;
	}

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public String getSourceType() {
		return sourceType;
	}

	public void setSourceType(String sourceType) {
		if (sourceType != null && !sourceType.equals("logger") && !sourceType.equals("table") && !sourceType.equals("stream"))
			throw new IllegalArgumentException();

		this.sourceType = sourceType;
	}

	public List<String> getSources() {
		return sources;
	}

	public void setSources(List<String> sources) {
		this.sources = sources;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

	public Date getModified() {
		return modified;
	}

	public void setModified(Date modified) {
		this.modified = modified;
	}

	@Override
	public String toString() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return "name=" + name + ", interval=" + interval + ", query=" + queryString + ", source_type=" + sourceType
				+ ", sources=" + sources + ", owner=" + owner + ", enabled=" + enabled + ", created=" + df.format(created)
				+ ", modified=" + df.format(modified) + ", description=" + description;
	}

}
