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

public class RotationStateHelper {
	public static Map<String, Object> serialize(Map<String, RotationState> rotationStates) {
		Map<String, Object> states = new HashMap<String, Object>();
		for (String key : rotationStates.keySet()) {
			RotationState rotationState = rotationStates.get(key);
			if (rotationState == null)
				continue;

			states.put(key, rotationState.serialize());
		}

		return states;
	}

	@SuppressWarnings("unchecked")
	public static Map<String, RotationState> deserialize(Map<String, Object> states) {
		Map<String, RotationState> rotationStates = new HashMap<String, RotationState>();

		for (String key : states.keySet()) {
			rotationStates.put(key, RotationState.deserialize((Map<String, Object>) states.get(key)));
		}

		return rotationStates;
	}
}
