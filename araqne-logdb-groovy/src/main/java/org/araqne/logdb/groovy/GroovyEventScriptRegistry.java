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
package org.araqne.logdb.groovy;

import java.util.List;

public interface GroovyEventScriptRegistry {
	/**
	 * List all event subscriptions
	 * 
	 * @param topicFilter
	 *            return matching subscription if filter is specified
	 */
	List<GroovyEventSubscription> getEventSubscriptions(String topicFilter);

	/**
	 * groovy event script will receive event after subscribed
	 */
	void subscribeEvent(GroovyEventSubscription subscription);

	/**
	 * groovy event script will stop receiving event after unsubscribed
	 */
	void unsubscribeEvent(GroovyEventSubscription subscription);

	/**
	 * reload event handler
	 * 
	 * @param scriptName
	 *            groovy script file name except .groovy extension
	 */
	void reloadScript(String scriptName);

	/**
	 * @param scriptName
	 *            groovy script file name except .groovy extension
	 * @return new event script instance
	 */
	GroovyEventScript newScript(String scriptName);
}
