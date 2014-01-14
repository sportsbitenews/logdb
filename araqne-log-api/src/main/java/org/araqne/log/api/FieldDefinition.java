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
package org.araqne.log.api;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.api.FieldOption;

public class FieldDefinition {
	@FieldOption(nullable = false)
	private String name;

	/**
	 * string, short, int, long, float, double, date, bool
	 */
	@FieldOption(nullable = false)
	private String type;

	/**
	 * 0 for unknown
	 */
	@FieldOption(nullable = false)
	private int length;

	public static FieldDefinition parse(String s) {
		if (s == null)
			throw new IllegalArgumentException("field definition should not be null");

		Pattern p = Pattern.compile("(\\S+)\\s+([^() ]+)(?:\\s*\\(\\s*(\\d+)\\s*\\))*");
		Matcher m = p.matcher(s);
		if (!m.find())
			throw new IllegalStateException("invalid field definition format: " + s);

		String fieldName = m.group(1);
		String type = m.group(2);
		int len = 0;
		if (m.group(3) != null)
			len = Integer.valueOf(m.group(3));

		return new FieldDefinition(fieldName, type, len);
	}

	public static boolean isValidType(String s) {
		return s.equals("string") || s.equals("short") || s.equals("int") || s.equals("long") || s.equals("float")
				|| s.equals("double") || s.equals("date") || s.equals("bool");
	}

	public FieldDefinition() {
	}

	public FieldDefinition(String name, String type) {
		this(name, type, 0);
	}

	public FieldDefinition(String name, String type, int length) {
		this.name = name;
		this.type = type;
		this.length = length;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	@Override
	public String toString() {
		if (length > 0)
			return name + " " + type + "(" + length + ")";
		return name + " " + type;
	}
}
