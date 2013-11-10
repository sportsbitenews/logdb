/*
 * Copyright 2010 NCHOVY
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
package org.araqne.log.api.impl;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.ServiceProperty;
import org.araqne.api.Script;
import org.araqne.api.ScriptFactory;
import org.araqne.log.api.LogNormalizerFactoryRegistry;
import org.araqne.log.api.LogNormalizerRegistry;
import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.log.api.LogParserRegistry;
import org.araqne.log.api.LogTransformerFactoryRegistry;
import org.araqne.log.api.LogTransformerRegistry;
import org.araqne.log.api.LoggerFactoryRegistry;
import org.araqne.log.api.LoggerRegistry;

@Component(name = "logapi-script-factory")
@Provides
public class LogApiScriptFactory implements ScriptFactory {
	@ServiceProperty(name = "alias", value = "logapi")
	private String alias;

	@Requires
	private LoggerRegistry loggerRegistry;

	@Requires
	private LoggerFactoryRegistry loggerFactoryRegistry;

	@Requires
	private LogParserFactoryRegistry parserFactoryRegistry;

	@Requires
	private LogNormalizerFactoryRegistry normalizerFactoryRegistry;

	@Requires
	private LogTransformerFactoryRegistry transformerFactoryRegistry;

	@Requires
	private LogParserRegistry parserRegistry;

	@Requires
	private LogNormalizerRegistry normalizerRegistry;

	@Requires
	private LogTransformerRegistry transformerRegistry;

	@Override
	public Script createScript() {
		return new LogApiScript(loggerFactoryRegistry, loggerRegistry, parserFactoryRegistry, normalizerFactoryRegistry,
				transformerFactoryRegistry, parserRegistry, normalizerRegistry, transformerRegistry);
	}
}
