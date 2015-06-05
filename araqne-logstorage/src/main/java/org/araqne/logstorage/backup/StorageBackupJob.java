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

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @since 2.2.7
 * @author xeraph
 * 
 */
public class StorageBackupJob {
	private StorageBackupRequest request;

	// sum bytes of all data files
	private long totalBytes;
	private Date submitAt;
	private boolean done;
	private boolean overwrite;
	private boolean incremental;
	private boolean move;
	private File dstFile;

	// table to files
	private Map<String, List<StorageFile>> storageFiles;
	private Map<String, List<StorageMediaFile>> mediaFiles;

	public StorageBackupRequest getRequest() {
		return request;
	}

	public void setRequest(StorageBackupRequest request) {
		this.request = request;
	}

	public long getTotalBytes() {
		return totalBytes;
	}

	public void setTotalBytes(long totalBytes) {
		this.totalBytes = totalBytes;
	}

	public Date getSubmitAt() {
		return submitAt;
	}

	public void setSubmitAt(Date submitAt) {
		this.submitAt = submitAt;
	}

	public boolean isDone() {
		return done;
	}

	public void setDone(boolean done) {
		this.done = done;
	}

	public boolean isOverwrite() {
		return overwrite;
	}

	public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}

	public boolean isIncremental() {
		return incremental;
	}

	public void setIncremental(boolean incremental) {
		this.incremental = incremental;
	}

	public boolean isMove() {
		return move;
	}

	public void setMove(boolean move) {
		this.move = move;
	}

	public File getDstFile() {
		return dstFile;
	}

	public void setDstPath(File dstFile) {
		this.dstFile = dstFile;
	}

	public Map<String, List<StorageFile>> getStorageFiles() {
		return storageFiles;
	}

	public void setStorageFiles(Map<String, List<StorageFile>> storageFiles) {
		this.storageFiles = storageFiles;
	}

	public Map<String, List<StorageMediaFile>> getMediaFiles() {
		return mediaFiles;
	}

	public void setMediaFiles(Map<String, List<StorageMediaFile>> sourceFiles) {
		this.mediaFiles = sourceFiles;
	}

}
