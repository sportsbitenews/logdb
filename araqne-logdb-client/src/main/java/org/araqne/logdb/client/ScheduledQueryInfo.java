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
package org.araqne.logdb.client;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * 예약된 쿼리를 표현합니다.
 * 
 * @since 0.9.5
 * @author xeraph@eediom.com
 * 
 */
public class ScheduledQueryInfo {
	private String guid = UUID.randomUUID().toString();
	private String title;
	private String cronSchedule;
	private String owner;
	private String queryString;
	private boolean saveResult;
	private boolean useAlert;
	private String alertQuery;

	private int suppressInterval;
	private String mailProfile;
	private String mailFrom;
	private String mailTo;
	private String mailSubject;

	private boolean enabled;
	private Date created = new Date();

	public String getGuid() {
		return guid;
	}

	public void setGuid(String guid) {
		this.guid = guid;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getCronSchedule() {
		return cronSchedule;
	}

	public void setCronSchedule(String cronSchedule) {
		this.cronSchedule = cronSchedule;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public boolean isSaveResult() {
		return saveResult;
	}

	public void setSaveResult(boolean saveResult) {
		this.saveResult = saveResult;
	}

	public boolean isUseAlert() {
		return useAlert;
	}

	public void setUseAlert(boolean useAlert) {
		this.useAlert = useAlert;
	}

	public String getAlertQuery() {
		return alertQuery;
	}

	public void setAlertQuery(String alertQuery) {
		this.alertQuery = alertQuery;
	}

	public int getSuppressInterval() {
		return suppressInterval;
	}

	public void setSuppressInterval(int suppressInterval) {
		this.suppressInterval = suppressInterval;
	}

	public String getMailProfile() {
		return mailProfile;
	}

	public void setMailProfile(String mailProfile) {
		this.mailProfile = mailProfile;
	}

	public String getMailFrom() {
		return mailFrom;
	}

	public void setMailFrom(String mailFrom) {
		this.mailFrom = mailFrom;
	}

	public String getMailTo() {
		return mailTo;
	}

	public void setMailTo(String mailTo) {
		this.mailTo = mailTo;
	}

	public String getMailSubject() {
		return mailSubject;
	}

	public void setMailSubject(String mailSubject) {
		this.mailSubject = mailSubject;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

	@Override
	public String toString() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return "guid=" + guid + ", title=" + title + ", cron=" + cronSchedule + ", owner=" + owner + ", query=" + queryString
				+ ", save_result=" + saveResult + ", use_alert=" + useAlert + ", alert_query=" + alertQuery
				+ ", suppress_interval=" + suppressInterval + ", mail_profile=" + mailProfile + ", mail_from=" + mailFrom
				+ ", mail_to=" + mailTo + ", mail_subject=" + mailSubject + ", enabled=" + enabled + ", created="
				+ df.format(created);
	}
}
