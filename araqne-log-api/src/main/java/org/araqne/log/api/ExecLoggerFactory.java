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
package org.araqne.log.api;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;

/**
 * @since 2.4.6
 * @author xeraph
 * 
 */
@Component(name = "exec-logger-factory")
@Provides
public class ExecLoggerFactory extends AbstractLoggerFactory {

	@Override
	public String getName() {
		return "exec";
	}

	@Override
	public String getDisplayName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "외부프로그램";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "外部プログラム";
		if(locale != null && locale.equals(Locale.CHINESE))
			return "外部程序";
		return "External Program";
	}

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "외부 프로그램의 표준 출력을 로그로 수집합니다.";
		if (locale != null && locale.equals(Locale.JAPANESE))
			return "外部プログラムの標準出力をログとして収集します。";
		if(locale != null && locale.equals(Locale.CHINESE))
			return "采集外部程序的标准输出内容。";
		return "Collect standard output of external program";
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return Arrays.asList(Locale.ENGLISH, Locale.KOREAN, Locale.JAPANESE, Locale.CHINESE);
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		LoggerConfigOption command = new MutableStringConfigType("command", 
				t("Command", "명령어", "コマンド","命令"), 
				t("Command to execute in shell", "쉘에서 실행할 명령어", 
						"シェルで実行するコマンド","命令行命令"), true);
	
		return Arrays.asList(command);
	}

	private Map<Locale, String> t(String enText, String koText, String jpText, String cnText) {
		Map<Locale, String> m = new HashMap<Locale, String>();
		m.put(Locale.ENGLISH, enText);
		m.put(Locale.KOREAN, koText);
		m.put(Locale.JAPANESE, jpText);
		m.put(Locale.CHINESE, cnText);
		return m;
	}

	@Override
	protected Logger createLogger(LoggerSpecification spec) {
		return new ExecLogger(spec, this);
	}
}
