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
package org.araqne.logdb.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.confdb.Config;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigService;
import org.araqne.confdb.Predicates;
import org.araqne.logdb.Procedure;
import org.araqne.logdb.ProcedureRegistry;

@Component(name = "logdb-procedure-registry")
@Provides
public class ProcedureRegistryImpl implements ProcedureRegistry {

	@Requires
	private ConfigService conf;

	private ConcurrentHashMap<String, Procedure> procedures = new ConcurrentHashMap<String, Procedure>();

	private Object dbLock = new Object();

	@Validate
	public void start() {
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
		for (Procedure p : db.findAll(Procedure.class).getDocuments(Procedure.class)) {
			procedures.put(p.getName(), p);
		}
	}

	@Invalidate
	public void stop() {
		procedures.clear();
	}

	@Override
	public Set<String> getProcedureNames() {
		return new HashSet<String>(procedures.keySet());
	}

	@Override
	public List<Procedure> getProcedures() {
		return new ArrayList<Procedure>(procedures.values());
	}

	@Override
	public Procedure getProcedure(String name) {
		if (name == null)
			return null;

		return procedures.get(name);
	}

	@Override
	public void createProcedure(Procedure procedure) {
		if (procedure == null)
			throw new IllegalArgumentException("procedure should not be null");

		Procedure old = procedures.putIfAbsent(procedure.getName(), procedure);
		if (old != null)
			throw new IllegalStateException("duplicated procedure name: " + procedure.getName());

		synchronized (dbLock) {
			ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
			db.add(procedure);
		}
	}

	@Override
	public void updateProcedure(Procedure procedure) {
		if (procedure == null)
			throw new IllegalArgumentException("procedure should not be null");

		if (procedures.get(procedure.getName()) == null)
			throw new IllegalStateException("procedure not found: " + procedure.getName());

		procedures.put(procedure.getName(), procedure);

		synchronized (dbLock) {
			ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
			Config c = db.findOne(Procedure.class, Predicates.field("name", procedure.getName()));
			if (c != null)
				db.update(c, procedure);
		}
	}

	@Override
	public void removeProcedure(String name) {
		if (name == null)
			throw new IllegalArgumentException("procedure name should not be null");

		Procedure old = procedures.remove(name);
		if (old == null)
			throw new IllegalStateException("procedure not found: " + name);

		synchronized (dbLock) {
			ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
			Config c = db.findOne(Procedure.class, Predicates.field("name", name));
			if (c != null) {
				c.remove();
			}
		}
	}
}
