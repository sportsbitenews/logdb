/*
 * Copyright 2012 Future Systems
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
package org.araqne.logdb.jython.impl;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.ServiceProperty;
import org.araqne.api.Script;
import org.araqne.api.ScriptFactory;
import org.araqne.logdb.jython.JythonLoggerScriptRegistry;
import org.araqne.logdb.jython.JythonParserScriptRegistry;
import org.araqne.logdb.jython.JythonQueryScriptRegistry;
import org.araqne.logdb.jython.JythonTransformerScriptRegistry;
import org.osgi.framework.BundleContext;

@Component(name = "logdb-jython-script-factory")
@Provides
public class LogdbJythonScriptFactory implements ScriptFactory {

	@SuppressWarnings("unused")
	@ServiceProperty(name = "alias", value = "logdb-jython")
	private String alias;

	@Requires
	private JythonQueryScriptRegistry queryScriptRegistry;

	@Requires
	private JythonLoggerScriptRegistry loggerScriptRegistry;

	@Requires
	private JythonTransformerScriptRegistry transformerScriptRegistry;

	@Requires
	private JythonParserScriptRegistry parserScriptRegistry;

	private BundleContext bc;

	public LogdbJythonScriptFactory(BundleContext bc) {
		this.bc = bc;
	}

	@Override
	public Script createScript() {
		return new LogdbJythonScript(bc, queryScriptRegistry, loggerScriptRegistry, transformerScriptRegistry,
				parserScriptRegistry);
	}
}
