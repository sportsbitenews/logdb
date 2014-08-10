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
package org.araqne.logdb.jms.logger;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.araqne.log.api.AbstractLoggerFactory;
import org.araqne.log.api.Logger;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.StringConfigType;
import org.araqne.logdb.jms.JmsProfileRegistry;

@Component(name = "jms-logger-factory")
@Provides
public class JmsLoggerFactory extends AbstractLoggerFactory {

	@Requires
	private JmsProfileRegistry profileRegistry;

	@Override
	public String getName() {
		return "jms";
	}

	@Override
	public String getDisplayName(Locale locale) {
		return "JMS Consumer";
	}

	@Override
	public String getDescription(Locale locale) {
		return "Receives the JMS messages sent to the destination";
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption profile = new StringConfigType("jms_profile", t("JMS Profile", "JMS 프로파일"), t("JMS profile name",
				"JMS 프로파일 이름"), true);
		return Arrays.asList(profile);
	}

	private Map<Locale, String> t(String en, String ko) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, en);
		m.put(Locale.KOREAN, ko);
		return m;
	}

	@Override
	protected Logger createLogger(LoggerSpecification spec) {
		return new JmsLogger(spec, this, profileRegistry);
	}
}
