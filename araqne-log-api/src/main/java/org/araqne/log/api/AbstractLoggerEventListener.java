/**
 * Copyright 2015 Eediom Inc.
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

import java.util.Map;

/**
 * @since 3.6.0
 * @author xeraph
 * 
 */
public class AbstractLoggerEventListener implements LoggerEventListener {

	@Override
	public void onStart(Logger logger) {
	}

	@Override
	public void onStop(Logger logger, LoggerStopReason reason) {
	}

	@Override
	public void onSetTimeRange(Logger logger) {
	}

	@Override
	public void onFailureChange(Logger logger) {
	}

	@Override
	public void onUpdated(Logger logger, Map<String, String> config) {
	}

	@Override
	public boolean dissentStart(Logger logger) {
		return false;
	}

	@Override
	public void onPend(Logger logger, LoggerStopReason reason) {
	}

}
