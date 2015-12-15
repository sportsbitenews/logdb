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

import java.io.IOException;
import java.util.Date;
import java.util.Set;

public interface QueryResult extends RowPipe {
	Date getEofDate();

	long getCount();

	void syncWriter() throws IOException;

	void closeWriter();

	void openWriter() throws IOException;
	
	void purge();

	boolean isStreaming();

	void setStreaming(boolean streaming);


	QueryResultSet getResultSet() throws IOException;

	Set<QueryResultCallback> getResultCallbacks();
}
