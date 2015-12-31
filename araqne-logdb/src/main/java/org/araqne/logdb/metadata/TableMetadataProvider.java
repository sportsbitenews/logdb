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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.log.api.FieldDefinition;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.FieldOrdering;
import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.MetadataCallback;
import org.araqne.logdb.MetadataProvider;
import org.araqne.logdb.MetadataService;
import org.araqne.logdb.Permission;
import org.araqne.logdb.Privilege;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.SecurityGroup;
import org.araqne.logstorage.LockKey;
import org.araqne.logstorage.LockStatus;
import org.araqne.logstorage.LogFileService;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogRetentionPolicy;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.StorageConfig;
import org.araqne.logstorage.TableConfig;
import org.araqne.logstorage.TableConfigSpec;
import org.araqne.logstorage.TableSchema;

@Component(name = "logdb-table-metadata")
public class TableMetadataProvider implements MetadataProvider, FieldOrdering {

	@Requires
	private LogTableRegistry tableRegistry;

	@Requires
	private AccountService accountService;

	@Requires
	private LogFileServiceRegistry lfsRegistry;

	@Requires
	private LogStorage storage;

	@Requires
	private MetadataService metadataService;

	@Requires
	private FunctionRegistry functionRegistry;

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
	public void verify(QueryContext context, String queryString) {
		MetadataQueryStringParser.getTableNames(context, tableRegistry, accountService, functionRegistry, queryString);
	}

	@Override
	public void query(QueryContext context, String queryString, MetadataCallback callback) {
		TableScanOption opt = MetadataQueryStringParser.getTableNames(context, tableRegistry, accountService, functionRegistry,
				queryString);
		List<String> targetTables = opt.getTableNames();

		for (String tableName : tableRegistry.getTableNames()) {
			if (targetTables.contains(tableName))
				if (accountService.checkPermission(context.getSession(), tableName, Permission.READ))
					writeTableInfo(context, tableName, callback);
		}
	}

	private void writeTableInfo(QueryContext context, String tableName, MetadataCallback callback) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("table", tableName);
		TableSchema s = tableRegistry.getTableSchema(tableName);

		StorageConfig primaryStorage = s.getPrimaryStorage();
		LogFileService lfs = lfsRegistry.getLogFileService(primaryStorage.getType());

		// primary storage
		m.put("primary_configs", marshal(lfs, s.getPrimaryStorage()));
		// replica storage
		m.put("replica_configs", marshal(lfs, s.getReplicaStorage()));

		// field definitions
		List<FieldDefinition> fields = s.getFieldDefinitions();
		if (fields != null) {
			for (FieldDefinition field : fields) {
				String line = null;
				if (field.getLength() > 0)
					line = field.getName() + "\t" + field.getType() + "(" + field.getLength() + ")";
				line = field.getName() + "\t" + field.getType();

				m.put("fields", line);
			}
		}

		m.put("metadata", s.getMetadata());

		// retention pollicy
		LogRetentionPolicy retentionPolicy = storage.getRetentionPolicy(tableName);
		String retention = null;
		if (retentionPolicy != null && retentionPolicy.getRetentionDays() > 0)
			retention = retentionPolicy.getRetentionDays() + "days";

		m.put("retention_policy", retention);

		m.put("data_path", storage.getTableDirectory(tableName).getAbsolutePath());

		LockStatus status = storage.lockStatus(new LockKey("script", tableName, null));
		m.put("is_locked", status.isLocked());
		if (status.isLocked()) {
			m.put("lock_owner", status.getOwner());
			m.put("lock_purpose", status.getPurposes().toArray(new String[0]));
			m.put("lock_reentcnt", status.getReentrantCount());
		} else {
			m.put("lock_owner", null);
			m.put("lock_purpose", null);
			m.put("lock_reentcnt", null);
		}

		List<Object> privileges = new ArrayList<Object>();
		for (Privilege p : accountService.getPrivileges(context.getSession(), null)) {
			if (!p.getTableName().equals(tableName))
				continue;

			Map<String, Object> privilege = new HashMap<String, Object>();
			privilege.put("login_name", p.getLoginName());
			List<String> permissions = new ArrayList<String>();
			for (Permission permission : p.getPermissions())
				permissions.add(permission.toString());
			privilege.put("permissions", permissions);
			privileges.add(privilege);
		}
		m.put("privileges", privileges);

		List<Map<String, Object>> groups = new ArrayList<Map<String, Object>>();
		for (SecurityGroup sg : accountService.getSecurityGroups()) {
			if (sg.getReadableTables().contains(tableName)) {
				Map<String, Object> group = new HashMap<String, Object>();
				group.put("guid", sg.getGuid());
				group.put("name", sg.getName());
				group.put("description", sg.getDescription());
				group.put("created", sg.getCreated());
				group.put("updated", sg.getUpdated());
				groups.add(group);
			}
		}
		m.put("security_groups", groups);

		callback.onPush(new Row(m));
	}

	private Map<String, Object> marshal(LogFileService lfs, StorageConfig storageConfig) {
		if (storageConfig == null)
			return null;

		Map<String, Object> m = new HashMap<String, Object>();
		m.put("type", storageConfig.getType());
		m.put("base_path", storageConfig.getBasePath());

		Map<String, String> configs = new HashMap<String, String>();
		for (TableConfig c : storageConfig.getConfigs()) {
			String value = null;
			if (c != null && c.getValues().size() > 1)
				value = c.getValues().toString();
			else if (c != null)
				value = c.getValue();
			configs.put(c.getKey(), value);
		}

		m.put("configs", configs);

		return m;
	}

	@Override
	public List<String> getFieldOrder() {
		return Arrays.asList("table", "compression", "crypto", "metadata", "replication_mode", "replication_table", "lock_owner",
				"lock_purpose", "lock_reentcnt", "retention_policy", "data_path");
	}

}
