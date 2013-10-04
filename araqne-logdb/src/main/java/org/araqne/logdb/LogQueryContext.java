/*
 * Copyright 2013 Future Systems
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
package org.araqne.logdb;

import java.util.HashMap;
import java.util.Map;

public class LogQueryContext {
	private Session session;
	private Map<String, Object> constants = new HashMap<String, Object>();

	public LogQueryContext(Session session) {
		this.session = session;
	}
	
	public Session getSession() {
		return session;
	}

	public Map<String, Object> getConstants() {
		return constants;
	}

}
