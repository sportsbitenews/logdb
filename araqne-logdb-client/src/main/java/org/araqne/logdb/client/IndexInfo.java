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

/**
 * 인덱스 설정을 표현합니다.
 * 
 * @author xeraph@eediom.com
 * 
 */
public class IndexInfo {
	private String tableName;
	private String indexName;
	private String tokenizerName;
	private Map<String, String> tokenizerConfigs = new HashMap<String, String>();
	private boolean useBloomFilter;
	private int bloomFilterCapacity0 = 1250000;
	private double bloomFilterErrorRate0 = 0.001f;
	private int bloomFilterCapacity1 = 10000000;
	private double bloomFilterErrorRate1 = 0.02f;
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

	public boolean isUseBloomFilter() {
		return useBloomFilter;
	}

	public void setUseBloomFilter(boolean useBloomFilter) {
		this.useBloomFilter = useBloomFilter;
	}

	public int getBloomFilterCapacity0() {
		return bloomFilterCapacity0;
	}

	public void setBloomFilterCapacity0(int bloomFilterCapacity0) {
		this.bloomFilterCapacity0 = bloomFilterCapacity0;
	}

	public double getBloomFilterErrorRate0() {
		return bloomFilterErrorRate0;
	}

	public void setBloomFilterErrorRate0(double bloomFilterErrorRate0) {
		this.bloomFilterErrorRate0 = bloomFilterErrorRate0;
	}

	public int getBloomFilterCapacity1() {
		return bloomFilterCapacity1;
	}

	public void setBloomFilterCapacity1(int bloomFilterCapacity1) {
		this.bloomFilterCapacity1 = bloomFilterCapacity1;
	}

	public double getBloomFilterErrorRate1() {
		return bloomFilterErrorRate1;
	}

	public void setBloomFilterErrorRate1(double bloomFilterErrorRate1) {
		this.bloomFilterErrorRate1 = bloomFilterErrorRate1;
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

		String bloomFilterConfig = "bloomfilter=" + useBloomFilter;
		if (useBloomFilter) {
			bloomFilterConfig += "[lv0: " + bloomFilterCapacity0 + ", " + bloomFilterErrorRate0 + ", ";
			bloomFilterConfig += "lv1: " + bloomFilterCapacity1 + ", " + bloomFilterErrorRate1 + "]";
		}

		return "table=" + tableName + ", index=" + indexName + "," + bloomFilterConfig + ", tokenizer=" + tokenizerName
				+ ", tokenizer configs=" + tokenizerConfigs + ", base path=" + basePath + ", min index day=" + s
				+ ", build past index=" + buildPastIndex;
	}

}
