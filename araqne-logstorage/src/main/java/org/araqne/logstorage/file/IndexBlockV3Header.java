/*
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
package org.araqne.logstorage.file;

import java.io.IOException;

public class IndexBlockV3Header {
	public static final int ITEM_SIZE = 28;
	
	// index header file offset
	public long fp;
	public long firstId;

	public long dataFp;
	public long minTime;
	public long maxTime;
	public int logCount;

	// except this block's log count
	public long ascLogCount;
	public long dscLogCount;

	public IndexBlockV3Header(long datafp, long minTime, long maxTime, int logCount, long firstId) throws IOException {
		this.dataFp = datafp;
		this.minTime = minTime;
		this.maxTime = maxTime;
		this.logCount = logCount;
		this.firstId = firstId;
	}

	@Override
	public String toString() {
		return "index block header, fp=" + fp + ", first_id=" + firstId + ", count=" + logCount + ", asc=" + ascLogCount
				+ ", dsc=" + dscLogCount + "]";
	}
}
