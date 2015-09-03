/*
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
package org.araqne.log.api.http.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.araqne.log.api.AbstractLoggerFactory;
import org.araqne.log.api.Logger;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.MutableStringConfigType;
import org.araqne.log.api.http.HttpPostLogService;

@Component(name = "http-post-logger-factory")
@Provides
public class HttpPostLoggerFactory extends AbstractLoggerFactory {

	@Requires
	private HttpPostLogService postLog;

	@Override
	public String getName() {
		return "http-post";
	}

	@Override
	public List<Locale> getLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDisplayGroup(Locale locale) {
		return "HTTP";
	}

	@Override
	public String getDisplayName(Locale locale) {
		return "HTTP POST";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "HTTP POST를 통해 로그를 수신합니다.";
		return "Collect logs through HTTP POST";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption callback = new MutableStringConfigType("callback", t("Callback", "콜백 이름"), t(
				"Name for /log/CALLBACK endpoint", "/log/콜백이름"), true);
		return Arrays.asList(callback);
	}

	private Map<Locale, String> t(String enText, String koText) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, enText);
		m.put(Locale.KOREAN, koText);
		return m;
	}

	@Override
	protected Logger createLogger(LoggerSpecification spec) {
		return new HttpPostLogger(spec, this, postLog);
	}

}
