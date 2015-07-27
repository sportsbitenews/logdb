/*
 * Copyright 2010 NCHOVY
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

import java.util.Collection;
import java.util.List;
import java.util.Locale;

import org.osgi.framework.BundleContext;

public interface LoggerFactory {
	/**
	 * logger factory registry calls onStart() when logger factory component is
	 * registered to OSGi service registry
	 */
	void onStart(BundleContext bc);

	/**
	 * logger factory registry calls onStop() when logger factory component is
	 * unregistering from OSGi service registry
	 */
	void onStop();

	LastStateService getLastStateService();

	String getFullName();

	String getNamespace();

	String getName();

	/**
	 * @since 3.6.0
	 */
	List<Locale> getLocales();
	
	/**
	 * @since 3.6.0
	 */
	String getDisplayGroup(Locale locale);

	@Deprecated
	Collection<Locale> getDisplayNameLocales();

	String getDisplayName(Locale locale);

	@Deprecated
	Collection<Locale> getDescriptionLocales();

	String getDescription(Locale locale);

	Collection<LoggerConfigOption> getConfigOptions();

	Logger newLogger(LoggerSpecification spec);

	void deleteLogger(String name);

	void deleteLogger(String namespace, String name);

	void addListener(LoggerFactoryEventListener callback);

	void removeListener(LoggerFactoryEventListener callback);

	boolean isAvailable();
}
