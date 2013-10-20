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
package org.araqne.logstorage.script;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import org.araqne.api.ScriptContext;
import org.araqne.logstorage.backup.BackupJob;
import org.araqne.logstorage.backup.BaseFile;
import org.araqne.logstorage.backup.Job;
import org.araqne.logstorage.backup.JobProgressMonitor;
import org.araqne.logstorage.backup.RestoreJob;

/**
 * @since 2.2.7
 * @author xeraph
 * 
 */
class BackupProgressPrinter implements JobProgressMonitor {
	private ScriptContext context;
	private boolean disabled = false;

	public BackupProgressPrinter(ScriptContext context) {
		this.context = context;
	}

	public boolean isDisabled() {
		return disabled;
	}

	public void setDisabled(boolean disabled) {
		this.disabled = disabled;
	}

	@Override
	public void onBeginTable(Job job, String tableName) {
		if (!disabled)
			context.println(getTimestamp() + ">> table [" + tableName + "] " + getType(job));
	}

	@Override
	public void onCompleteTable(Job job, String tableName) {
		if (!disabled)
			context.println(getTimestamp() + "<< table [" + tableName + "] " + getType(job));
	}

	@Override
	public void onBeginFile(Job job, BaseFile bf) {
		if (!disabled)
			context.println(getTimestamp() + "> file [" + bf.getTableName() + ":" + bf.getFileName() + "] " + getType(job));
	}

	@Override
	public void onCompleteFile(Job job, BaseFile bf) {
		if (!disabled)
			context.println(getTimestamp() + "< file [" + bf.getTableName() + ":" + bf.getFileName() + "] " + getType(job));
	}

	@Override
	public void onBeginJob(Job job) {
		if (!disabled)
			context.println(getTimestamp() + getType(job) + " started table [" + getTableNames(job) + "], ["
					+ formatNumber(job.getTotalBytes()) + "] bytes");
	}

	@Override
	public void onCompleteJob(Job job) {
		if (!disabled) {
			context.println(getTimestamp() + getType(job) + " completed, table " + getTableNames(job) + ", ["
					+ formatNumber(job.getTotalBytes()) + "] bytes");
			context.println("Press ctrl-c to exit monitor");
		}
	}

	private String getTimestamp() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return "[" + df.format(new Date()) + "] ";
	}

	private String getType(Job job) {
		if (job instanceof BackupJob)
			return "backup";
		else
			return "restore";
	}

	private Set<String> getTableNames(Job job) {
		if (job instanceof BackupJob)
			return ((BackupJob) job).getSourceFiles().keySet();
		else
			return ((RestoreJob) job).getSourceFiles().keySet();
	}

	private String formatNumber(long bytes) {
		DecimalFormat formatter = new DecimalFormat("###,###");
		return formatter.format(bytes);
	}

}
