/*
 * Copyright 2011 Future Systems
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

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.ServiceProperty;
import org.araqne.api.Script;
import org.araqne.api.ScriptFactory;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.CsvLookupRegistry;
import org.araqne.logdb.LogQueryService;
import org.araqne.logdb.LogQueryScriptRegistry;
import org.araqne.logdb.LookupHandlerRegistry;
import org.araqne.logdb.SavedResultManager;

@Component(name = "logdb-script-factory")
@Provides
public class LogDBScriptFactory implements ScriptFactory {
	@SuppressWarnings("unused")
	@ServiceProperty(name = "alias", value = "logdb")
	private String alias;

	@Requires
	private LogQueryService qs;

	@Requires
	private LogQueryScriptRegistry scriptRegistry;

	@Requires
	private LookupHandlerRegistry lookup;

	@Requires
	private CsvLookupRegistry csvLookup;

	@Requires
	private AccountService accountService;

	@Requires
	private SavedResultManager savedResultManager;

	@Override
	public Script createScript() {
		return new LogDBScript(qs, scriptRegistry, lookup, csvLookup, accountService, savedResultManager);
	}
}
