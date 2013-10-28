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
package org.araqne.logstorage.backup;

import java.util.Locale;
import java.util.Map;

/**
 * @since 2.3.0
 * @author xeraph
 * 
 */
public class StorageBackupConfigSpec {
	private String key;
	private Map<Locale, String> displayNames;
	private Map<Locale, String> descriptions;

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Map<Locale, String> getDisplayNames() {
		return displayNames;
	}

	public void setDisplayNames(Map<Locale, String> displayNames) {
		this.displayNames = displayNames;
	}

	public Map<Locale, String> getDescriptions() {
		return descriptions;
	}

	public void setDescriptions(Map<Locale, String> descriptions) {
		this.descriptions = descriptions;
	}
}
