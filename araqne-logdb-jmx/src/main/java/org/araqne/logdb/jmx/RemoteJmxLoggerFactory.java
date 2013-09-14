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
package org.araqne.logdb.jmx;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.log.api.AbstractLoggerFactory;
import org.araqne.log.api.IntegerConfigType;
import org.araqne.log.api.Logger;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.StringConfigType;

@Component(name = "remote-jmx-logger-factory")
@Provides
public class RemoteJmxLoggerFactory extends AbstractLoggerFactory {

	@Override
	public String getName() {
		return "remotejmx";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "원격 JMX";
		return "Remote JMX";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale.equals(Locale.KOREAN))
			return "RMI 통신을 통해 원격지의 JMX 에이전트를 쿼리합니다.";
		return "Query JMX Agent using RMI protocol";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN);
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption host = new StringConfigType("host", t("Host address", "호스트 주소"), t("domain name or ip address",
				"도메인 주소 또는 IP 주소"), true);
		LoggerConfigOption port = new IntegerConfigType("port", t("Port", "포트"), t("RMI port number", "RMI 포트 주소"), true);
		LoggerConfigOption user = new StringConfigType("user", t("User", "사용자 계정"), t("RMI account", "RMI 접속 계정"), true);
		LoggerConfigOption password = new StringConfigType("password", t("Password", "암호"), t("RMI password", "RMI 접속 암호"), true);
		LoggerConfigOption objName = new StringConfigType("obj_name", t("Object Name", "개체 이름"), t("JMX object name", "JMX 개체 이름"), true);
		LoggerConfigOption attrNames = new StringConfigType("attr_names", t("Attribute Names", "속성 이름 목록"), t(
				"comma separated attribute names, all attributes by default", "쉼표로 구분되는 속성 이름 목록, 미설정 시 전체 속성 수집"), false);

		return Arrays.asList(host, port, user, password, objName, attrNames);
	}

	private Map<Locale, String> t(String en, String ko) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, en);
		m.put(Locale.KOREAN, ko);
		return m;
	}

	@Override
	protected Logger createLogger(LoggerSpecification spec) {
		try {
			return new RemoteJmxLogger(spec, this);
		} catch (IOException e) {
			throw new IllegalStateException("cannot create remote jmx logger", e);
		}
	}
}
