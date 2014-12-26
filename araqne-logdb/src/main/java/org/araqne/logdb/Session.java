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

import java.util.Date;
import java.util.Set;

public interface Session {
	String getGuid();

	String getLoginName();

	Date getCreated();

	/**
	 * @since 1.0.0
	 */
	boolean isAdmin();

	/**
	 * @since 2.4.53
	 */
	Set<String> getPropertyKeys();

	/**
	 * @since 2.4.53
	 */
	Object getProperty(String name);

	/**
	 * @since 2.4.53
	 */
	void setProperty(String name, Object value);

	/**
	 * @since 2.4.53
	 */
	void unsetProperty(String name);
}
