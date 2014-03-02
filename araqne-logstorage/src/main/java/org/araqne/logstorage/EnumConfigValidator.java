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
package org.araqne.logstorage;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EnumConfigValidator implements TableConfigValidator {
	private Set<String> enums = new HashSet<String>();

	public EnumConfigValidator(Set<String> enums) {
		this.enums = enums;
	}

	@Override
	public void validate(String key, List<String> values) {
		if (values == null || values.isEmpty())
			throw new IllegalArgumentException("empty value for " + key);

		if (values.size() > 1)
			throw new IllegalArgumentException("too many values for " + key);

		if (!enums.contains(values.get(0)))
			throw new IllegalArgumentException("invalid value for " + key + ", " + enums + " options are available");
	}
}
