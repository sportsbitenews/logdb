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
import org.araqne.logdb.AbstractAccountEventListener;
import org.araqne.logdb.Account;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.Procedure;
import org.araqne.logdb.ProcedureRegistry;
import org.araqne.logdb.SecurityGroup;
import org.araqne.logdb.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "logdb-procedure-registry")
@Provides
public class ProcedureRegistryImpl implements ProcedureRegistry {
	private final Logger slog = LoggerFactory.getLogger(ProcedureRegistryImpl.class);

	@Requires
	private ConfigService conf;

	@Requires
	private AccountService accountService;

	private OrphanCleaner cleaner = new OrphanCleaner();

	private ConcurrentHashMap<String, Procedure> procedures = new ConcurrentHashMap<String, Procedure>();

	private Object dbLock = new Object();

	@Validate
	public void start() {
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
		for (Procedure p : db.findAll(Procedure.class).getDocuments(Procedure.class)) {
			procedures.put(p.getName(), p);
		}

		accountService.addListener(cleaner);
	}

	@Invalidate
	public void stop() {
		if (accountService != null)
			accountService.removeListener(cleaner);

		procedures.clear();
	}

	@Override
	public boolean isGranted(String procedureName, String loginName) {
		Procedure p = procedures.get(procedureName);
		if (p == null)
			throw new IllegalStateException("procedure not found: " + procedureName);

		if (accountService.isAdmin(loginName))
			return true;

		if (p.getOwner().equals(loginName) || p.getGrants().contains(loginName))
			return true;

		for (String guid : p.getGrantGroups()) {
			SecurityGroup group = accountService.getSecurityGroup(guid);
			if (group != null && group.getAccounts().contains(loginName))
				return true;
		}

		return false;
	}

	@Override
	public Set<String> getProcedureNames() {
		return new HashSet<String>(procedures.keySet());
	}

	@Override
	public List<Procedure> getProcedures() {
		return getProcedures(null);
	}

	@Override
	public List<Procedure> getProcedures(String loginName) {
		List<Procedure> l = new ArrayList<Procedure>();
		for (Procedure p : procedures.values()) {
			if (loginName != null && !isGranted(p.getName(), loginName))
				continue;

			l.add(p.clone());
		}

		return l;
	}

	@Override
	public Procedure getProcedure(String name) {
		if (name == null)
			return null;

		Procedure p = procedures.get(name);
		if (p == null)
			return null;

		return p.clone();
	}

	@Override
	public void createProcedure(Procedure procedure) {
		if (procedure == null)
			throw new IllegalArgumentException("procedure should not be null");

		Procedure old = procedures.putIfAbsent(procedure.getName(), procedure);
		if (old != null)
			throw new IllegalStateException("duplicated procedure name: " + procedure.getName());

		filterGrants(procedure);

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

		filterGrants(procedure);

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

	private void filterGrants(Procedure p) {
		Set<String> filteredGrants = new HashSet<String>();

		for (String s : p.getGrants()) {
			if (accountService.getAccount(s) != null)
				filteredGrants.add(s);
		}

		Set<String> filteredGrantGroups = new HashSet<String>();
		for (String s : p.getGrantGroups()) {
			if (accountService.getSecurityGroup(s) != null)
				filteredGrantGroups.add(s);
		}

		p.setGrants(filteredGrants);
		p.setGrantGroups(filteredGrantGroups);
	}

	private class OrphanCleaner extends AbstractAccountEventListener {

		@Override
		public void onRemoveAccount(Session session, Account account) {
			for (Procedure p : procedures.values()) {
				if (p.getOwner().equals(account.getLoginName())) {
					removeProcedure(p.getName());
					slog.info("araqne logdb: dropped procedure [{}] by remove cascade of user [{}]", p.getName(),
							account.getLoginName());
					continue;
				}

				if (p.getGrants().contains(account.getLoginName())) {
					p.getGrants().remove(account.getLoginName());
					updateProcedure(p);
					slog.info("araqne logdb: revoked procedure [{}] by remove cascade of user [{}]", p.getName(),
							account.getLoginName());
				}
			}
		}

		@Override
		public void onRemoveAccounts(Session session, List<Account> accounts) {
			Set<String> loginNames = new HashSet<String>();
			for (Account account : accounts)
				loginNames.add(account.getLoginName());

			for (Procedure p : procedures.values()) {
				if (loginNames.contains(p.getOwner())) {
					removeProcedure(p.getName());
					slog.info("araqne logdb: dropped procedure [{}] by remove cascade of user [{}]", p.getName(), p.getOwner());
					continue;
				}

				boolean contains = false;
				for (String loginName : loginNames) {
					if (p.getGrants().contains(loginName)) {
						p.getGrants().remove(loginName);
						contains = true;
					}
				}

				if (contains) {
					updateProcedure(p);
					slog.info("araqne logdb: revoked procedure [{}] by remove cascade of users", p.getName());
				}
			}
		}

		@Override
		public void onRemoveSecurityGroup(Session session, SecurityGroup group) {
			for (Procedure p : procedures.values()) {
				if (p.getGrantGroups().contains(group.getGuid())) {
					p.getGrantGroups().remove(group.getGuid());
					updateProcedure(p);
					slog.info("araqne logdb: revoked procedure [{}] by remove cascade of security group [guid: {}, name: {}]",
							new Object[] { p.getName(), group.getGuid(), group.getName() });
				}
			}
		}

	}
}
