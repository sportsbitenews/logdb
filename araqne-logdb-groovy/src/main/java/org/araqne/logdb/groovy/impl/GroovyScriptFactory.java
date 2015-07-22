/*
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
package org.araqne.logdb.groovy.impl;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.ServiceProperty;
import org.araqne.api.Script;
import org.araqne.api.ScriptFactory;
import org.araqne.logdb.groovy.GroovyEventScriptRegistry;

@Component(name = "logdb-groovy-script-factory")
@Provides
public class GroovyScriptFactory implements ScriptFactory {

	@ServiceProperty(name = "alias", value = "groovy")
	private String alias;

	@Requires
	private GroovyEventScriptRegistry eventScriptRegistry;

	@Override
	public Script createScript() {
		return new GroovyScript(eventScriptRegistry);
	}
}
