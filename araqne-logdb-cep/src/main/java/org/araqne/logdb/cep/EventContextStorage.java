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
import java.util.Map;
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

	EventContext addContext(EventContext ctx);

	void removeContext(EventKey key, EventContext ctx, EventCause cause);

	void advanceTime(String host, long now);

	void clearClocks();

	void clearContexts();

	void clearContexts(String topic);

	void addSubscriber(String topic, EventSubscriber subscriber);

	void removeSubscriber(String topic, EventSubscriber subscriber);

	void removeContexts(Map<EventKey, EventContext> contexts, EventCause removal);

	Map<EventKey, EventContext> getContexts(Set<EventKey> key);
	
	void registerContext(EventContext ctx);

	void registerContexts(List<EventContext> contexts);
}
