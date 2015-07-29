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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public interface LoggerConfigOption {
	String getName();

	Collection<Locale> getDisplayNameLocales();

	String getDisplayName(Locale locale);

	Collection<Locale> getDescriptionLocales();

	String getDescription(Locale locale);

	String getType();

	boolean isRequired();

	Collection<Locale> getDefaultValueLocales();

	String getDefaultValue(Locale locale);

	void validate(Object value);

	Object parse(String value);
	
	String getSubtype();
	
	//TODO: logpresso 브랜치에서 작업할 때 추가할것
	/*
	 * public class LoggerConfigSerializer {
	public static Map<String, Object> getConfigOption(LoggerConfigOption o) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("name", o.getName());
		m.put("type", o.getType());
		m.put("is_required", o.isRequired());
		m.put("display_names", getDisplayNames(o));
		m.put("descriptions", getDescriptions(o));
		m.put("default_values", getDefaultValues(o));
		
		m.put("subtype", o.getSubtype());
		
		return m;
	}
	 */
}
