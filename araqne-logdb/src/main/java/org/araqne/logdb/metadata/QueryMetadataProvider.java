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
package org.araqne.logdb.metadata;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.MetadataCallback;
import org.araqne.logdb.MetadataProvider;
import org.araqne.logdb.MetadataService;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryService;
import org.araqne.logdb.Row;
import org.araqne.logdb.impl.QueryHelper;

@Component(name = "logdb-query-metadata")
public class QueryMetadataProvider implements MetadataProvider {

	@Requires
	private MetadataService metadataService;

	@Requires
	private QueryService queryService;

	@Override
	public String getType() {
		return "queries";
	}

	@Validate
	public void start() {
		metadataService.addProvider(this);
	}

	@Invalidate
	public void stop() {
		if (metadataService != null)
			metadataService.removeProvider(this);
	}

	@Override
	public void verify(QueryContext context, String queryString) {
	}

	@Override
	public void query(QueryContext context, String queryString, MetadataCallback callback) {
		boolean admin = context.getSession().isAdmin();

		for (Query query : queryService.getQueries()) {
			String owner = null;
			if (query.getContext() != null && query.getContext().getSession() != null)
				owner = query.getContext().getSession().getLoginName();

			if (admin || (owner != null && owner.equals(context.getSession().getLoginName())))
				callback.onPush(new Row(QueryHelper.getQuery(query)));
		}
	}
}
