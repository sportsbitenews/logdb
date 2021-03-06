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
import java.util.Map;

public class LoggerConfigValidator {
	private LoggerConfigValidator() {
	}

	public static void validate(Collection<LoggerConfigOption> configTypes, Map<String, String> props) {
		for (LoggerConfigOption type : configTypes) {
			Object value = props.get(type.getName());
			if (value == null && type.isRequired())
				throw new IllegalArgumentException(type.getName() + " is required");

			if (value != null)
				type.validate(value);
		}
	}
}
