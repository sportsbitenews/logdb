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
package org.araqne.logdb.metadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.MetadataCallback;
import org.araqne.logdb.MetadataProvider;
import org.araqne.logdb.MetadataService;
import org.araqne.logdb.Privilege;
import org.araqne.logstorage.LogTableRegistry;

@Component(name = "logdb-table-metadata")
public class TableMetadataProvider implements MetadataProvider {

	@Requires
	private LogTableRegistry tableRegistry;

	@Requires
	private AccountService accountService;

	@Requires
	private MetadataService metadataService;

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
	public String getType() {
		return "tables";
	}

	@Override
	public void verify(LogQueryContext context, String queryString) {
	}

	@Override
	public void query(LogQueryContext context, String queryString, MetadataCallback callback) {
		if (context.getSession().isAdmin()) {
			for (String tableName : tableRegistry.getTableNames()) {
				writeTableInfo(tableName, callback);
			}
		} else {
			List<Privilege> privileges = accountService.getPrivileges(context.getSession(), context.getSession().getLoginName());
			for (Privilege p : privileges) {
				if (p.getPermissions().size() > 0 && tableRegistry.exists(p.getTableName())) {
					writeTableInfo(p.getTableName(), callback);
				}
			}
		}
	}

	private void writeTableInfo(String tableName, MetadataCallback callback) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("table", tableName);
		callback.onLog(new LogMap(m));
	}

}
