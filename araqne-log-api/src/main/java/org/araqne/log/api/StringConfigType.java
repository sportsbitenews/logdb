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

import java.util.Locale;
import java.util.Map;

public class StringConfigType extends AbstractConfigType {

	public StringConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions, boolean required) {
		super(name, displayNames, descriptions, required);
	}

	public StringConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions, boolean required,
			Map<Locale, String> defaultValues) {
		super(name, displayNames, descriptions, required, defaultValues);
	}

	public StringConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions, boolean required,
			Subtype subtype) {
		super(name, displayNames, descriptions, required, subtype);
	}

	public StringConfigType(String name, Map<Locale, String> displayNames, Map<Locale, String> descriptions, boolean required,
			Map<Locale, String> defaultValues, Subtype subtype) {
		super(name, displayNames, descriptions, required, defaultValues, subtype);
	}

	@Override
	public String getType() {
		return "string";
	}

	@Override
	public Object parse(String value) {
		return value;
	}
}
