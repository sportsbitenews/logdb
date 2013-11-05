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
package org.araqne.logdb.impl;

import java.util.Collection;

public class Strings {
	private Strings() {
	}

	public static String join(Collection<?> tokens, String sep) {
		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (Object s : tokens) {
			if (i++ != 0)
				sb.append(sep);
			sb.append(s);
		}
		return sb.toString();
	}
}
