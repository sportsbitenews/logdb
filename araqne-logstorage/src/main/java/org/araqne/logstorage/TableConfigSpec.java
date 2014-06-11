package org.araqne.logstorage;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

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
public class TableConfigSpec {
	public static enum Type {
		STRING, MULTI_STRING
	}

	private Type type = Type.STRING;
	private String key;
	private boolean optional;
	private boolean updatable;
	private Map<Locale, String> displayNames;
	private Map<Locale, String> descriptions;
	private TableConfigValidator validator;

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public boolean isOptional() {
		return optional;
	}

	public void setOptional(boolean optional) {
		this.optional = optional;
	}

	public boolean isUpdatable() {
		return updatable;
	}

	public void setUpdatable(boolean updatable) {
		this.updatable = updatable;
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

	public TableConfigValidator getValidator() {
		return validator;
	}

	public void setValidator(TableConfigValidator validator) {
		this.validator = validator;
	}

	public static Map<Locale, String> locales(String en, String ko) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, en);
		m.put(Locale.KOREAN, ko);
		return m;
	}
}
