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
package org.araqne.log.api;

import java.util.List;

public interface LastStateService {
	List<LastState> getStates();

	LastState getState(String loggerName);

	void setState(LastState state);

	/**
	 * delete json state file
	 * 
	 * @param loggerName
	 *            full qualified name
	 */
	void deleteState(String loggerName);

	void addListener(LastStateListener listener);

	void removeListener(LastStateListener listener);
}
