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
import java.util.List;
import java.util.Map;

/**
 * @since 0.10.3
 * @author xeraph
 * 
 */
public class ShortQuery {
	private ShortQuery() {
	}

	public static List<Map<String, Object>> query(Session session, LogQueryService queryService, String query) throws IOException {
		LogQuery q = null;

		try {
			q = queryService.createQuery(session, query);
			queryService.startQuery(q.getId());

			do {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
				}
			} while (!q.isEnd());

			return q.getResultAsList();
		} finally {
			if (q != null)
				queryService.removeQuery(q.getId());
		}
	}
}
