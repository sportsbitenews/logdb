/**
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

import java.util.HashMap;
import java.util.Map;

public class RotationState {
	private String firstLine;
	private long lastPosition;
	private long lastLength;

	public RotationState(String firstLine, long lastPosition, long lastLength) {
		this.firstLine = firstLine;
		this.lastPosition = lastPosition;
		this.lastLength = lastLength;
	}

	public static RotationState deserialize(Map<String, Object> m) {
		String firstLine = (String) m.get("first_line");
		long lastPosition = Long.valueOf(m.get("last_position").toString());
		long lastLength = Long.valueOf(m.get("last_length").toString());
		return new RotationState(firstLine, lastPosition, lastLength);
	}

	public Map<String, Object> serialize() {
		HashMap<String, Object> m = new HashMap<String, Object>(4);
		m.put("first_line", firstLine);
		m.put("last_position", lastPosition);
		m.put("last_length", lastLength);
		return m;
	}

	public String getFirstLine() {
		return firstLine;
	}

	public void setFirstLine(String firstLine) {
		this.firstLine = firstLine;
	}

	public long getLastPosition() {
		return lastPosition;
	}

	public void setLastPosition(long lastPosition) {
		this.lastPosition = lastPosition;
	}

	public long getLastLength() {
		return lastLength;
	}

	public void setLastLength(long lastLength) {
		this.lastLength = lastLength;
	}
}
