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
package org.araqne.logstorage.backup;

import java.util.Date;
import java.util.Set;
import java.util.UUID;

/**
 * @since 2.2.7
 * @author xeraph
 * 
 */
public class StorageBackupRequest {
	private String guid;
	private StorageBackupType type;
	private Set<String> tableNames;
	private Date from;
	private Date to;
	private StorageBackupMedia media;
	private StorageBackupProgressMonitor progressMonitor;

	public StorageBackupRequest(StorageBackupType type) {
		this.guid = UUID.randomUUID().toString();
		this.type = type;
	}

	public String getGuid() {
		return guid;
	}

	public StorageBackupType getType() {
		return type;
	}

	public void setType(StorageBackupType type) {
		this.type = type;
	}

	public Set<String> getTableNames() {
		return tableNames;
	}

	public void setTableNames(Set<String> tableNames) {
		this.tableNames = tableNames;
	}

	public Date getFrom() {
		return from;
	}

	public void setFrom(Date from) {
		this.from = from;
	}

	public Date getTo() {
		return to;
	}

	public void setTo(Date to) {
		this.to = to;
	}

	public StorageBackupMedia getMedia() {
		return media;
	}

	public void setMedia(StorageBackupMedia media) {
		this.media = media;
	}

	public StorageBackupProgressMonitor getProgressMonitor() {
		return progressMonitor;
	}

	public void setProgressMonitor(StorageBackupProgressMonitor progressMonitor) {
		this.progressMonitor = progressMonitor;
	}
}
