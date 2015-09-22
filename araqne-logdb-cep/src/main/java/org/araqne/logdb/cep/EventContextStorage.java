/*
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
package org.araqne.logdb.cep;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public interface EventContextStorage {
	String getName();

	/**
	 * return hosts which contains external clock
	 */
	Set<String> getHosts();

	EventClock<? extends EventClockItem> getClock(String host);

	Iterator<EventKey> getContextKeys();

	Iterator<EventKey> getContextKeys(String topic);

	EventContext getContext(EventKey key);

	void advanceTime(String host, long now);

	void clearClocks();

	void clearContexts();

	void clearContexts(String topic);

	void addSubscriber(String topic, EventSubscriber subscriber);

	void removeSubscriber(String topic, EventSubscriber subscriber);

	void removeContext(EventKey key, EventCause cause);

	void removeContexts(List<EventKey> contexts, EventCause removal);

	List<EventContext> getContexts(Set<EventKey> key);

	void storeContext(EventContext ctx);

	void storeContexts(List<EventContext> contexts);
	
	void addContextVariable(EventKey evtKey, String key, Object value);
}
