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
package org.araqne.logdb.client;

import java.util.ArrayList;
import java.util.List;

/**
 * 쿼리 파이프라인을 구성하는 개별 커맨드의 정보를 표현합니다.
 * 
 * @author xeraph@eediom.com
 * 
 */
public class LogQueryCommand {
	private String name;
	private String command;
	private String status;
	private long pushCount;

	/**
	 * @since 0.9.1
	 */
	private List<LogQueryCommand> commands = new ArrayList<LogQueryCommand>();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCommand() {
		return command;
	}

	public void setCommand(String command) {
		this.command = command;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public long getPushCount() {
		return pushCount;
	}

	public void setPushCount(long pushCount) {
		this.pushCount = pushCount;
	}

	/**
	 * @since 0.9.1
	 */
	public List<LogQueryCommand> getCommands() {
		return commands;
	}

	/**
	 * @since 0.9.1
	 */
	public void setCommands(List<LogQueryCommand> commands) {
		this.commands = commands;
	}

	@Override
	public String toString() {
		return "[" + status + "] " + command + " - passed " + pushCount;
	}
}
