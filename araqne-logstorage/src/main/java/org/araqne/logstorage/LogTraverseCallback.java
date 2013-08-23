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

package org.araqne.logstorage;

public abstract class LogTraverseCallback {
	private long processedCnt;
	
	public LogTraverseCallback() {
		this.processedCnt = 0;
	}
	
	public boolean onLog(Log log) {
		if (!isMatch(log))
			return false;
		
		
		processLog(log);
		processedCnt++;
		return true;
	}
	
	public long getProcessedCount() {
		return processedCnt;
	}
	
	abstract public boolean isMatch(Log log);
	
	abstract public void interrupt();
	abstract public boolean isInterrupted();
	
	abstract protected void processLog(Log log);
}
