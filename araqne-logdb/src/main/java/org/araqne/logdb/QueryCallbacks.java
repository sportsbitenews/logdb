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
package org.araqne.logdb;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public class QueryCallbacks {

	private Set<QueryResultCallback> resultCallbacks = new CopyOnWriteArraySet<QueryResultCallback>();
	private Set<QueryStatusCallback> statusCallbacks = new CopyOnWriteArraySet<QueryStatusCallback>();
	private Set<QueryTimelineCallback> timelineCallbacks = new CopyOnWriteArraySet<QueryTimelineCallback>();

	public Set<QueryStatusCallback> getStatusCallbacks() {
		return statusCallbacks;
	}

	public Set<QueryResultCallback> getResultCallbacks() {
		return resultCallbacks;
	}

	public Set<QueryTimelineCallback> getTimelineCallbacks() {
		return timelineCallbacks;
	}
}
