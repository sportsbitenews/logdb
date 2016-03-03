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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * @since 2.6.0
 * @author xeraph
 * 
 */
public abstract class AbstractLogParserFactory implements LogParserFactory {

	@Override
	public Collection<Locale> getDisplayNameLocales() {
		return getLocales();
	}

	@Override
	public Collection<Locale> getDescriptionLocales() {
		return getLocales();
	}

	@Override
	public List<Locale> getLocales() {
		return Arrays.asList(Locale.ENGLISH);
	}

	@Override
	public String getDisplayGroup(Locale locale) {
		return null;
	}

	@Override
	public Collection<LoggerConfigOption> getConfigOptions() {
		return new ArrayList<LoggerConfigOption>();
	}
}
