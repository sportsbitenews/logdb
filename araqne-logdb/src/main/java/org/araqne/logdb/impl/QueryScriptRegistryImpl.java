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
package org.araqne.logdb.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.QueryScript;
import org.araqne.logdb.QueryScriptFactory;
import org.araqne.logdb.QueryScriptRegistry;

@Component(name = "logdb-query-script-registry")
@Provides
public class QueryScriptRegistryImpl implements QueryScriptRegistry {

	private ConcurrentMap<String, ConcurrentMap<String, QueryScriptFactory>> workspaceToScripts;

	@Validate
	public void start() {
		workspaceToScripts = new ConcurrentHashMap<String, ConcurrentMap<String, QueryScriptFactory>>();
	}

	@Override
	public Set<String> getWorkspaceNames() {
		return Collections.unmodifiableSet(workspaceToScripts.keySet());
	}

	@Override
	public void createWorkspace(String name) {
		workspaceToScripts.putIfAbsent(name, new ConcurrentHashMap<String, QueryScriptFactory>());
	}

	@Override
	public void dropWorkspace(String name) {
		workspaceToScripts.remove(name);
	}

	@Override
	public Set<String> getScriptFactoryNames(String workspace) {
		ConcurrentMap<String, QueryScriptFactory> scripts = workspaceToScripts.get(workspace);
		if (scripts == null)
			return null;

		return Collections.unmodifiableSet(scripts.keySet());
	}

	@Override
	public List<QueryScriptFactory> getScriptFactories(String workspace) {
		ConcurrentMap<String, QueryScriptFactory> scripts = workspaceToScripts.get(workspace);
		if (scripts == null)
			return null;

		return new ArrayList<QueryScriptFactory>(scripts.values());
	}

	@Override
	public QueryScriptFactory getScriptFactory(String workspace, String name) {
		ConcurrentMap<String, QueryScriptFactory> scripts = workspaceToScripts.get(workspace);
		if (scripts == null)
			return null;

		return scripts.get(name);
	}

	@Override
	public QueryScript newScript(String workspace, String name, Map<String, String> params) {
		ConcurrentMap<String, QueryScriptFactory> scripts = workspaceToScripts.get(workspace);
		if (scripts == null)
			throw new IllegalStateException("script not found: " + name);

		QueryScriptFactory factory = scripts.get(name);
		if (factory == null)
			throw new IllegalStateException("script not found: " + name);

		return factory.create(params);
	}

	@Override
	public void addScriptFactory(String workspace, String name, QueryScriptFactory factory) {
		ConcurrentMap<String, QueryScriptFactory> scripts = new ConcurrentHashMap<String, QueryScriptFactory>();
		ConcurrentMap<String, QueryScriptFactory> oldScripts = workspaceToScripts.putIfAbsent(workspace, scripts);
		if (oldScripts != null)
			scripts = oldScripts;

		QueryScriptFactory old = scripts.putIfAbsent(name, factory);
		if (old != null)
			throw new IllegalStateException("log script already exists: " + name);
	}

	@Override
	public void removeScriptFactory(String workspace, String name) {
		ConcurrentMap<String, QueryScriptFactory> scripts = workspaceToScripts.get(workspace);
		if (scripts == null)
			return;

		scripts.remove(name);
	}
}
