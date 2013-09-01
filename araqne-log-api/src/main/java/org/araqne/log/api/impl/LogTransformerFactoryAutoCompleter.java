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
package org.araqne.log.api.impl;

import java.util.ArrayList;
import java.util.List;

import org.araqne.api.ScriptAutoCompletion;
import org.araqne.api.ScriptAutoCompletionHelper;
import org.araqne.api.ScriptSession;
import org.araqne.log.api.LogTransformerFactory;
import org.araqne.log.api.LogTransformerFactoryRegistry;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;

/**
 * @since 2.6.0
 * @author xeraph
 * 
 */
public class LogTransformerFactoryAutoCompleter implements ScriptAutoCompletionHelper {
	@Override
	public List<ScriptAutoCompletion> matches(ScriptSession session, String prefix) {
		ArrayList<ScriptAutoCompletion> l = new ArrayList<ScriptAutoCompletion>();

		// support since core 2.6.0
		BundleContext bc = (BundleContext) session.getProperty("bc");
		if (bc == null)
			return l;

		ServiceReference ref = bc.getServiceReference(LogTransformerFactoryRegistry.class.getName());
		if (ref == null)
			return l;

		LogTransformerFactoryRegistry registry = (LogTransformerFactoryRegistry) bc.getService(ref);
		if (registry == null)
			return l;

		for (LogTransformerFactory factory : registry.getFactories()) {
			if (factory.getName().startsWith(prefix))
				l.add(new ScriptAutoCompletion(factory.getName()));
		}

		return l;
	}

}
