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
package org.araqne.logdb.client;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class IndexInfo {
	private String tableName;
	private String indexName;
	private String tokenizerName;
	private Map<String, String> tokenizerConfigs = new HashMap<String, String>();
	private Date minIndexDay;
	private String basePath;
	private boolean buildPastIndex;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public String getTokenizerName() {
		return tokenizerName;
	}

	public void setTokenizerName(String tokenizerName) {
		this.tokenizerName = tokenizerName;
	}

	public Map<String, String> getTokenizerConfigs() {
		return tokenizerConfigs;
	}

	public void setTokenizerConfigs(Map<String, String> tokenizerConfigs) {
		this.tokenizerConfigs = tokenizerConfigs;
	}

	public Date getMinIndexDay() {
		return minIndexDay;
	}

	public void setMinIndexDay(Date minIndexDay) {
		this.minIndexDay = minIndexDay;
	}

	public String getBasePath() {
		return basePath;
	}

	public void setBasePath(String basePath) {
		this.basePath = basePath;
	}

	public boolean isBuildPastIndex() {
		return buildPastIndex;
	}

	public void setBuildPastIndex(boolean buildPastIndex) {
		this.buildPastIndex = buildPastIndex;
	}

	@Override
	public String toString() {
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
		String s = null;
		if (minIndexDay != null)
			s = f.format(minIndexDay);

		return "table=" + tableName + ", index=" + indexName + ", tokenizer=" + tokenizerName + ", tokenizer configs="
				+ tokenizerConfigs + ", base path=" + basePath + ", min index day=" + s + ", build past index=" + buildPastIndex;
	}

}
