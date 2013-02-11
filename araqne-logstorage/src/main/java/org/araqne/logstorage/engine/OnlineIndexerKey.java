/*
 * Copyright 2013 Future Systems
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
package org.araqne.logstorage.engine;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @since 0.9
 * @author xeraph
 */
class OnlineIndexerKey {
	public int indexId;
	public Date day;

	public int tableId;
	public String tableName;
	public String indexName;

	public OnlineIndexerKey(int indexId, Date day, int tableId, String tableName, String indexName) {
		this.indexId = indexId;
		this.day = day;

		this.tableId = tableId;
		this.tableName = tableName;
		this.indexName = indexName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((day == null) ? 0 : day.hashCode());
		result = prime * result + indexId;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OnlineIndexerKey other = (OnlineIndexerKey) obj;
		if (day == null) {
			if (other.day != null)
				return false;
		} else if (!day.equals(other.day))
			return false;
		if (indexId != other.indexId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		return "table=" + tableName + ", index=" + indexName + ", day=" + dateFormat.format(day);
	}

}