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

import java.util.List;

import org.araqne.logstorage.index.InvertedIndexItem;

/**
 * @since 0.9
 * @author xeraph
 */
class IndexCursorItem {
	public int tableId;
	public int indexId;
	public String tableName;
	public String indexName;
	public List<InvertedIndexItem> buffer;

	public IndexCursorItem(int tableId, int indexId, String tableName, String indexName, List<InvertedIndexItem> buffer) {
		this.tableId = tableId;
		this.indexId = indexId;
		this.tableName = tableName;
		this.indexName = indexName;
		this.buffer = buffer;
	}
}
