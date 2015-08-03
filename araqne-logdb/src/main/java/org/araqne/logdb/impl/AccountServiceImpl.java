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
package org.araqne.logdb.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.api.PrimitiveConverter;
import org.araqne.confdb.Config;
import org.araqne.confdb.ConfigCollection;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigIterator;
import org.araqne.confdb.ConfigService;
import org.araqne.confdb.ConfigTransaction;
import org.araqne.confdb.Predicates;
import org.araqne.logdb.Account;
import org.araqne.logdb.AccountEventListener;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.AuthServiceNotLoadedException;
import org.araqne.logdb.ExternalAuthService;
import org.araqne.logdb.Permission;
import org.araqne.logdb.Privilege;
import org.araqne.logdb.SecurityGroup;
import org.araqne.logdb.Session;
import org.araqne.logdb.SessionEventListener;
import org.araqne.logstorage.TableEventListener;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "logdb-account")
@Provides(specifications = { AccountService.class })
public class AccountServiceImpl implements AccountService, TableEventListener {
	private final Logger logger = LoggerFactory.getLogger(AccountServiceImpl.class);
	private static final String DB_NAME = "araqne-logdb";
	private static final String DEFAULT_MASTER_ACCOUNT = "araqne";
	private static final char[] SALT_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray();

	@Requires
	private ConfigService conf;

	@Requires
	private LogTableRegistry tableRegistry;

	private ConcurrentMap<String, Session> sessions;
	private ConcurrentMap<String, Account> localAccounts;
	private ConcurrentMap<String, SecurityGroup> securityGroups;
	private ConcurrentMap<String, ExternalAuthService> authServices;

	private String selectedExternalAuth;
	private CopyOnWriteArraySet<SessionEventListener> sessionListeners;
	private CopyOnWriteArraySet<AccountEventListener> accountListeners;
	private String instanceGuid;

	public AccountServiceImpl() {
		sessions = new ConcurrentHashMap<String, Session>();
		localAccounts = new ConcurrentHashMap<String, Account>();
		securityGroups = new ConcurrentHashMap<String, SecurityGroup>();
		authServices = new ConcurrentHashMap<String, ExternalAuthService>();
		sessionListeners = new CopyOnWriteArraySet<SessionEventListener>();
		accountListeners = new CopyOnWriteArraySet<AccountEventListener>();
	}

	@Validate
	public void start() {
		tableRegistry.addListener(this);
		sessions.clear();
		localAccounts.clear();
		authServices.clear();

		ConfigDatabase db = conf.ensureDatabase(DB_NAME);

		// load accounts
		for (Account account : db.findAll(Account.class).getDocuments(Account.class)) {
			localAccounts.put(account.getLoginName(), account);
		}

		// load security groups
		for (SecurityGroup group : db.findAll(SecurityGroup.class).getDocuments(SecurityGroup.class)) {
			securityGroups.put(group.getGuid(), group);
		}

		// generate default 'araqne' account if not exists
		if (!localAccounts.containsKey(DEFAULT_MASTER_ACCOUNT)) {
			String salt = randomSalt(10);
			Account account = new Account(DEFAULT_MASTER_ACCOUNT, salt, HashUtils.hash(salt, Account.DEFAULT_HASH_ALGORITHM));
			db.add(account);
			localAccounts.put(DEFAULT_MASTER_ACCOUNT, account);
		}

		// load external auth service config
		ConfigCollection col = db.ensureCollection("global_config");
		Config c = col.findOne(null);
		if (c != null) {
			@SuppressWarnings("unchecked")
			Map<String, Object> m = (Map<String, Object>) c.getDocument();
			selectedExternalAuth = (String) m.get("external_auth");
		}

		instanceGuid = UUID.randomUUID().toString();
	}

	@Invalidate
	public void stop() {
		if (tableRegistry != null)
			tableRegistry.removeListener(this);
	}

	@Override
	public List<Session> getSessions() {
		return new ArrayList<Session>(sessions.values());
	}

	@Override
	public Session getSession(String guid) {
		return sessions.get(guid);
	}

	@Override
	public boolean isAdmin(String loginName) {
		Account account = ensureAccount(loginName);
		return account.isAdmin();
	}

	@Override
	public void grantAdmin(Session session, String loginName) {
		if (!session.isAdmin())
			throw new IllegalStateException("no permission");

		Account account = ensureAccount(loginName);
		account.setAdmin(true);
		updateAccount(account);

		for (AccountEventListener listener : accountListeners) {
			try {
				listener.onGrantAdmin(session, account);
			} catch (Throwable t) {
				logger.warn("araqne logdb: account event listener should not throw any exception", t);
			}
		}
	}

	@Override
	public void revokeAdmin(Session session, String loginName) {
		if (!session.isAdmin())
			throw new IllegalStateException("no permission");

		if (session.getLoginName().equals(loginName))
			throw new IllegalStateException("cannot revoke current admin session");

		Account account = ensureAccount(loginName);
		account.setAdmin(false);
		updateAccount(account);

		for (AccountEventListener listener : accountListeners) {
			try {
				listener.onRevokeAdmin(session, account);
			} catch (Throwable t) {
				logger.warn("araqne logdb: account event listener should not throw any exception", t);
			}
		}
	}

	@Override
	public List<Privilege> getPrivileges(Session session, String loginName) {
		verifyNotNull(session, "session");

		if (!sessions.containsKey(session.getGuid()))
			throw new IllegalStateException("invalid session");

		// allow own info check or master admin only
		if (!checkOwner(session, loginName))
			throw new IllegalStateException("no permission");

		List<Privilege> privileges = new ArrayList<Privilege>();
		if (loginName != null) {
			checkAccountIncludingExternal(loginName);

			Account account = ensureAccount(loginName);
			for (String tableName : account.getReadableTables()) {
				privileges.add(new Privilege(loginName, tableName, Arrays.asList(Permission.READ)));
			}
		} else {
			ConfigDatabase db = conf.ensureDatabase(DB_NAME);
			for (Account account : db.findAll(Account.class).getDocuments(Account.class))
				for (String tableName : account.getReadableTables())
					privileges.add(new Privilege(account.getLoginName(), tableName, Arrays.asList(Permission.READ)));

		}
		return privileges;
	}

	@Override
	public void setPrivileges(Session session, List<Privilege> privileges) {
		verifyNotNull(privileges, "privileges");
		verifyAdminSession(session);

		Map<String, Account> accounts = new HashMap<String, Account>();
		for (Privilege privilege : privileges) {
			String loginName = privilege.getLoginName();
			Account account = accounts.get(loginName);
			if (account == null) {
				account = ensureAccount(loginName);
				account.getReadableTables().clear();
			}

			account.getReadableTables().add(privilege.getTableName());
			accounts.put(loginName, account);
		}

		ConfigDatabase db = conf.ensureDatabase(DB_NAME);
		ConfigIterator it = null;
		Map<String, Config> configs = new HashMap<String, Config>();
		try {
			it = db.find(Account.class, Predicates.in("login_name", accounts.keySet()));
			while (it.hasNext()) {
				Config c = it.next();
				String loginName = c.getDocument(Account.class).getLoginName();
				configs.put(loginName, c);
			}
		} finally {
			if (it != null)
				it.close();
		}

		ConfigTransaction xact = null;
		try {
			xact = db.beginTransaction();
			for (String loginName : accounts.keySet()) {
				Config c = configs.get(loginName);
				Account account = accounts.get(loginName);
				if (c == null)
					db.add(xact, account);
				else
					db.update(xact, c, account, false);
			}
			xact.commit("araqne-logdb", "set accounts");

			for (String loginName : accounts.keySet())
				localAccounts.putIfAbsent(loginName, accounts.get(loginName));
		} catch (Throwable t) {
			if (xact != null)
				xact.rollback();
			throw new IllegalStateException(t);
		}
	}

	@Override
	public void setPrivileges(Session session, String loginName, List<Privilege> privileges) {
		verifyNotNull(loginName, "loginName");
		verifyNotNull(privileges, "privileges");

		verifyAdminSession(session);

		Account account = ensureAccount(loginName);
		List<String> tables = account.getReadableTables();
		tables.clear();

		for (Privilege privilege : privileges)
			if (privilege.getLoginName() != null && privilege.getLoginName().equals(loginName))
				tables.add(privilege.getTableName());

		updateAccount(account);
	}

	private void verifyAdminSession(Session session) {
		verifyNotNull(session, "session");

		if (!sessions.containsKey(session.getGuid()))
			throw new IllegalStateException("invalid session");

		// master admin only
		if (!session.isAdmin())
			throw new IllegalStateException("no permission");
	}

	private boolean checkOwner(Session session, String loginName) {
		Account account = ensureAccount(session.getLoginName());
		if (account.isAdmin())
			return true;

		if (loginName == null)
			return false;

		return loginName.equals(session.getLoginName());
	}

	@Override
	public Set<String> getAccountNames() {
		return new HashSet<String>(localAccounts.keySet());
	}

	@Override
	public Account getAccount(String name) {
		return localAccounts.get(name);
	}

	@Override
	public boolean verifyPassword(String loginName, String password) {
		verifyNotNull(loginName, "login name");
		verifyNotNull(password, "password");

		Account account = localAccounts.get(loginName);
		// try local login first
		if (account != null && account.getAuthServiceName() == null) {
			// salted hash
			String hash = account.getPassword();
			String salt = account.getSalt();

			return hash.equals(HashUtils.hash(password + salt, account.getHashAlgorithm()));
		} else if (selectedExternalAuth != null) {
			// try external login
			ExternalAuthService auth = authServices.get(selectedExternalAuth);
			if (auth == null)
				throw new AuthServiceNotLoadedException(selectedExternalAuth);

			return auth.verifyPassword(loginName, password);
		} else
			throw new IllegalStateException("account not found");
	}

	@Override
	public Session newSession(String loginName) {
		verifyNotNull(loginName, "login name");

		Account account = ensureAccount(loginName);
		return registerSession(account);
	}

	@Override
	public Session login(String loginName, String hash, String nonce) {
		verifyNotNull(loginName, "login name");
		verifyNotNull(hash, "hash");
		verifyNotNull(nonce, "nonce");

		Account account = localAccounts.get(loginName);
		if (account == null)
			throw new IllegalStateException("account-not-found");

		String password = account.getPassword();
		if (!hash.equals(HashUtils.hash(password + nonce, account.getHashAlgorithm()))) {
			throw new IllegalStateException("invalid-password");
		}

		return registerSession(account);
	}

	@Override
	public Session login(String loginName, String password) {
		if (!verifyPassword(loginName, password))
			throw new IllegalStateException("invalid password");

		Account account = ensureAccount(loginName);
		return registerSession(account);
	}

	private Session registerSession(Account account) {
		String guid = UUID.randomUUID().toString();
		Session session = new SessionImpl(guid, account.getLoginName(), account.isAdmin());
		sessions.put(guid, session);

		// invoke callbacks
		for (SessionEventListener listener : sessionListeners) {
			try {
				listener.onLogin(session);
			} catch (Throwable t) {
				logger.warn("araqne logdb: session event listener should not throw any exception", t);
			}
		}

		return session;
	}

	@Override
	public void logout(Session session) {
		if (sessions.remove(session.getGuid()) == null)
			throw new IllegalStateException("session not found: " + session.getGuid());

		// invoke callbacks
		for (SessionEventListener listener : sessionListeners) {
			try {
				listener.onLogout(session);
			} catch (Throwable t) {
				logger.warn("araqne logdb: session event listener should not throw any exception", t);
			}
		}
	}

	@Override
	public void createAccount(Session session, String loginName, String password) {
		verifyNotNull(session, "session");
		verifyNotNull(loginName, "login name");
		verifyNotNull(password, "password");

		if (localAccounts.containsKey(loginName))
			throw new IllegalStateException("duplicated login name");

		if (!sessions.containsKey(session.getGuid()))
			throw new IllegalStateException("invalid session");

		if (!session.isAdmin())
			throw new IllegalStateException("no permission");

		// check database
		ConfigDatabase db = conf.ensureDatabase(DB_NAME);
		Config c = db.findOne(Account.class, Predicates.field("login_name", loginName));
		if (c != null)
			throw new IllegalStateException("duplicated login name");

		String salt = randomSalt(10);
		String hash = HashUtils.hash(password + salt, Account.DEFAULT_HASH_ALGORITHM);
		Account account = new Account(loginName, salt, hash);

		Account old = localAccounts.putIfAbsent(account.getLoginName(), account);
		if (old != null && old.getAuthServiceName() == null)
			throw new IllegalStateException("duplicated login name");

		db.add(account);

		for (AccountEventListener listener : accountListeners) {
			try {
				listener.onCreateAccount(session, account);
			} catch (Throwable t) {
				logger.warn("araqne logdb: account event listener should not throw any exception", t);
			}
		}
	}

	@Override
	public void changePassword(Session session, String loginName, String password) {
		verifyNotNull(session, "session");
		verifyNotNull(loginName, "login name");
		verifyNotNull(password, "password");

		if (!localAccounts.containsKey(loginName))
			throw new IllegalStateException("account not found");

		if (!sessions.containsKey(session.getGuid()))
			throw new IllegalStateException("invalid session");

		// check if owner or master
		if (!checkOwner(session, loginName))
			throw new IllegalStateException("no permission");

		Account account = localAccounts.get(loginName);

		String hash = HashUtils.hash(password + account.getSalt(), account.getHashAlgorithm());
		account.setPassword(hash);

		updateAccount(account);
	}

	private void updateAccount(Account account) {
		ConfigDatabase db = conf.ensureDatabase(DB_NAME);
		Config c = db.findOne(Account.class, Predicates.field("login_name", account.getLoginName()));
		if (c != null) {
			c.setDocument(PrimitiveConverter.serialize(account));
			c.update();
		} else {
			db.add(account);
		}

		localAccounts.putIfAbsent(account.getLoginName(), account);
	}

	@Override
	public void removeAccount(Session session, String loginName) {
		verifyNotNull(loginName, "login name");

		if (!localAccounts.containsKey(loginName))
			throw new IllegalStateException("account not found");

		if (session != null) {
			if (!sessions.containsKey(session.getGuid()))
				throw new IllegalStateException("invalid session");

			if (session.getLoginName().equals(loginName))
				throw new IllegalStateException("cannot delete your own account");

			// master admin only
			if (!session.isAdmin())
				throw new IllegalStateException("no permission");
		}

		Account account = localAccounts.remove(loginName);

		// drop all sessions
		for (Session s : new ArrayList<Session>(sessions.values())) {
			if (s.getLoginName().equals(loginName))
				sessions.remove(s.getGuid());
		}

		// delete from database
		ConfigDatabase db = conf.ensureDatabase(DB_NAME);
		Config c = db.findOne(Account.class, Predicates.field("login_name", loginName));
		if (c != null)
			c.remove();

		// delete from security groups
		for (SecurityGroup group : securityGroups.values()) {
			if (group.getAccounts().contains(loginName)) {
				group.getAccounts().remove(loginName);
				updateSecurityGroup(null, group);
			}
		}

		for (AccountEventListener listener : accountListeners) {
			try {
				listener.onRemoveAccount(session, account);
			} catch (Throwable t) {
				logger.warn("araqne logdb: account event listener should not throw any exception", t);
			}
		}
	}

	@Override
	public void removeAccounts(Session session, Set<String> loginNames) {
		verifyNotNull(loginNames, "login names");
		if (session != null)
			verifyAdminSession(session);

		List<Account> accounts = new ArrayList<Account>();
		for (String loginName : loginNames) {
			Account account = localAccounts.remove(loginName);
			if (account != null)
				accounts.add(account);
		}

		for (Session s : new ArrayList<Session>(sessions.values())) {
			if (loginNames.contains(s.getLoginName()))
				sessions.remove(s.getGuid());
		}

		ConfigDatabase db = conf.ensureDatabase(DB_NAME);
		ConfigIterator it = null;
		List<Config> configs = null;
		try {
			it = db.find(Account.class, Predicates.in("login_name", loginNames));
			configs = it.getConfigs(0, Integer.MAX_VALUE);
		} finally {
			if (it != null)
				it.close();
		}

		if (configs == null || configs.isEmpty())
			return;

		ConfigTransaction xact = null;
		try {
			xact = db.beginTransaction();
			for (Config c : configs)
				db.remove(xact, c, false);

			xact.commit("araqne-logdb", "remove accounts");
		} catch (Throwable t) {
			if (xact != null)
				xact.rollback();
			throw new IllegalStateException(t);
		}

		for (AccountEventListener listener : accountListeners) {
			try {
				listener.onRemoveAccounts(session, accounts);
			} catch (Throwable t) {
				logger.warn("araqne logdb: account event listener should not throw any exception", t);
			}
		}
	}

	@Override
	public Set<String> getSecurityGroupGuids() {
		return new HashSet<String>(securityGroups.keySet());
	}

	@Override
	public List<SecurityGroup> getSecurityGroups() {
		List<SecurityGroup> l = new ArrayList<SecurityGroup>();
		for (SecurityGroup g : securityGroups.values())
			l.add(g.clone());
		return l;
	}

	@Override
	public SecurityGroup getSecurityGroup(String guid) {
		SecurityGroup old = securityGroups.get(guid);
		if (old == null)
			return null;

		return old.clone();
	}

	@Override
	public void createSecurityGroup(Session session, SecurityGroup group) {
		if (session != null && !session.isAdmin()) {
			throw new IllegalStateException("no permission");
		}

		verifySecurityGroup(group);

		synchronized (securityGroups) {

			for (SecurityGroup o : securityGroups.values()) {
				// check duplicated name
				if (o.getName().equals(group.getName()))
					throw new IllegalStateException("duplicated security group name: " + group.getName());

				// check duplicated guid
				if (o.getGuid().equals(group.getGuid()))
					throw new IllegalStateException("duplicated security group guid: " + group.getGuid());
			}

			ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
			db.add(group, "araqne-logdb", "added security group [guid: " + group.getGuid() + ", name: " + group.getName() + "]");

			securityGroups.put(group.getGuid(), group);
		}

		// invoke callbacks
		for (AccountEventListener listener : accountListeners) {
			try {
				listener.onCreateSecurityGroup(session, group.clone());
			} catch (Throwable t) {
				logger.warn("araqne logdb: account listener should not throw any exception", t);
			}
		}
	}

	@Override
	public void updateSecurityGroup(Session session, SecurityGroup group) {
		verifySecurityGroup(group);

		SecurityGroup old = null;
		synchronized (securityGroups) {
			old = securityGroups.get(group.getGuid());
			if (old == null)
				throw new IllegalStateException("security group not found: " + group.getGuid());

			for (SecurityGroup o : securityGroups.values()) {
				if (!o.getGuid().equals(group.getGuid()) && o.getName().equals(group.getName()))
					throw new IllegalStateException("duplicated security group name: " + group.getName());
			}

			old.setName(group.getName());
			old.setDescription(group.getDescription());
			old.setAccounts(new HashSet<String>(group.getAccounts()));
			old.setReadableTables(new HashSet<String>(group.getReadableTables()));
			old.setUpdated(new Date());

			ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
			Config c = db.findOne(SecurityGroup.class, Predicates.field("guid", group.getGuid()));
			if (c == null)
				throw new IllegalStateException("security group not found at confdb: " + group.getGuid());

			db.update(c, group, false, "araqne-logdb",
					"updated security group [guid: " + group.getGuid() + ", name: " + group.getName() + "]");
		}

		// invoke callbacks
		for (AccountEventListener listener : accountListeners) {
			try {
				listener.onUpdateSecurityGroup(session, old.clone());
			} catch (Throwable t) {
				logger.warn("araqne logdb: account listener should not throw any exception", t);
			}
		}
	}

	private void verifySecurityGroup(SecurityGroup group) {
		if (group.getName() == null)
			throw new IllegalArgumentException("security group name should not be null");

		if (group.getGuid() == null)
			throw new IllegalArgumentException("security group guid should not be null");

		// check if table exists
		for (String tableName : group.getReadableTables()) {
			if (!tableRegistry.exists(tableName))
				throw new IllegalStateException("table not found: " + tableName);
		}

		syncAccounts(group.getAccounts());
	}

	private void syncAccounts(Set<String> loginNames) {
		Set<String> targetLoginNames = new HashSet<String>();
		for (String loginName : loginNames) {
			if (localAccounts.containsKey(loginName))
				continue;

			targetLoginNames.add(loginName);
		}

		List<Account> accounts = new ArrayList<Account>();
		if (selectedExternalAuth != null) {
			ExternalAuthService auth = authServices.get(selectedExternalAuth);
			if (auth != null)
				accounts = auth.findAccounts(targetLoginNames);
		}

		if (accounts.isEmpty())
			return;

		ConfigDatabase db = conf.ensureDatabase(DB_NAME);
		ConfigTransaction xact = null;
		try {
			xact = db.beginTransaction();
			for (Account account : accounts) {
				db.add(xact, account);
			}

			xact.commit("araqne-logdb", "sync accounts with external auth [" + selectedExternalAuth + "]");

			for (Account account : accounts)
				localAccounts.putIfAbsent(account.getLoginName(), account);
		} catch (Throwable t) {
			if (xact != null)
				xact.rollback();
			throw new IllegalStateException(t);
		}
	}

	@Override
	public void removeSecurityGroup(Session session, String guid) {
		SecurityGroup old = null;
		synchronized (securityGroups) {
			old = securityGroups.remove(guid);
			if (old == null)
				throw new IllegalStateException("security group not found: " + guid);

			ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
			Config c = db.findOne(SecurityGroup.class, Predicates.field("guid", guid));
			if (c != null)
				db.remove(c, false, "araqne-logdb", "removed security group [guid: " + old.getGuid() + ", name: " + old.getName()
						+ "]");
		}

		// invoke callbacks
		for (AccountEventListener listener : accountListeners) {
			try {
				listener.onRemoveSecurityGroup(session, old.clone());
			} catch (Throwable t) {
				logger.warn("araqne logdb: account listener should not throw any exception", t);
			}
		}
	}

	@Override
	public boolean checkPermission(Session session, String tableName, Permission permission) {
		verifyNotNull(session, "session");
		verifyNotNull(tableName, "table name");
		verifyNotNull(permission, "permission");

		if (permission != Permission.READ)
			throw new UnsupportedOperationException();

		// allow dummy login
		if (session.isAdmin())
			return true;

		String loginName = session.getLoginName();
		Account account = ensureAccount(loginName);
		boolean b = account.getReadableTables().contains(tableName);
		if (b)
			return true;

		// check security groups
		for (SecurityGroup g : securityGroups.values()) {
			if (!g.getAccounts().contains(loginName))
				continue;

			if (g.getReadableTables().contains(tableName))
				return true;
		}

		return false;
	}

	@Override
	public void grantPrivilege(Session session, String loginName, String tableName, Permission... permissions) {
		verifyNotNull(session, "session");
		verifyNotNull(loginName, "login name");
		verifyNotNull(tableName, "table name");

		if (permissions.length == 0)
			return;

		checkAccountIncludingExternal(loginName);

		if (!sessions.containsKey(session.getGuid()))
			throw new IllegalStateException("invalid session");

		// master admin only
		if (!session.isAdmin())
			throw new IllegalStateException("no permission");

		if (!tableRegistry.exists(tableName))
			throw new IllegalStateException("table not found");

		Account account = ensureAccount(loginName);
		if (account.getReadableTables().contains(tableName))
			return;

		account.getReadableTables().add(tableName);
		updateAccount(account);

		for (AccountEventListener listener : accountListeners) {
			try {
				listener.onGrantPrivilege(session, loginName, tableName, permissions);
			} catch (Throwable t) {
				logger.warn("araqne logdb: account event listener should not throw any exception", t);
			}
		}
	}

	@Override
	public void revokePrivilege(Session session, String loginName, String tableName, Permission... permissions) {
		verifyNotNull(session, "session");
		verifyNotNull(loginName, "login name");
		verifyNotNull(tableName, "table name");

		if (permissions.length == 0)
			return;

		checkAccountIncludingExternal(loginName);

		if (!sessions.containsKey(session.getGuid()))
			throw new IllegalStateException("invalid session");

		// master admin only
		if (!session.isAdmin())
			throw new IllegalStateException("no permission");

		if (!tableRegistry.exists(tableName))
			throw new IllegalStateException("table not found");

		Account account = ensureAccount(loginName);
		if (!account.getReadableTables().contains(tableName))
			return;

		account.getReadableTables().remove(tableName);
		updateAccount(account);

		for (AccountEventListener listener : accountListeners) {
			try {
				listener.onRevokePrivilege(session, loginName, tableName, permissions);
			} catch (Throwable t) {
				logger.warn("araqne logdb: account event listener should not throw any exception", t);
			}
		}
	}

	private Account ensureAccount(String loginName) {
		Account account = localAccounts.get(loginName);
		if (account != null)
			return account;

		if (selectedExternalAuth != null) {
			ExternalAuthService auth = authServices.get(selectedExternalAuth);
			if (auth != null && auth.verifyUser(loginName)) {
				account = new Account();
				account.setLoginName(loginName);
				account.setAuthServiceName(selectedExternalAuth);

				localAccounts.put(loginName, account);
				return account;
			}
		}

		throw new IllegalStateException("account not found: " + loginName);
	}

	private void checkAccountIncludingExternal(String loginName) {
		if (!localAccounts.containsKey(loginName)) {
			if (selectedExternalAuth != null) {
				ExternalAuthService auth = authServices.get(selectedExternalAuth);
				if (auth != null && auth.verifyUser(loginName))
					return;
			}

			throw new IllegalStateException("account not found");
		}
	}

	private void verifyNotNull(Object o, String name) {
		if (o == null)
			throw new IllegalArgumentException(name + " should not be null");
	}

	private String randomSalt(int saltLength) {
		StringBuilder salt = new StringBuilder(saltLength);
		Random rand = new Random();
		for (int i = 0; i < saltLength; i++)
			salt.append(SALT_CHARS[rand.nextInt(SALT_CHARS.length)]);
		return salt.toString();
	}

	@Override
	public ExternalAuthService getUsingAuthService() {
		if (selectedExternalAuth == null)
			return null;

		return authServices.get(selectedExternalAuth);
	}

	@Override
	public void useAuthService(String name) {
		if (name != null && !authServices.containsKey(name))
			throw new IllegalStateException("external auth service not found: " + name);

		ConfigDatabase db = conf.ensureDatabase(DB_NAME);
		ConfigCollection col = db.ensureCollection("global_config");
		Config c = col.findOne(null);

		if (c != null) {
			@SuppressWarnings("unchecked")
			Map<String, Object> doc = (Map<String, Object>) c.getDocument();
			doc.put("external_auth", name);
			c.setDocument(doc);
			c.update();
		} else {
			Map<String, Object> doc = new HashMap<String, Object>();
			doc.put("external_auth", name);
			col.add(doc);
		}

		selectedExternalAuth = name;
	}

	@Override
	public List<ExternalAuthService> getAuthServices() {
		return new ArrayList<ExternalAuthService>(authServices.values());
	}

	@Override
	public ExternalAuthService getAuthService(String name) {
		return authServices.get(name);
	}

	@Override
	public void registerAuthService(ExternalAuthService auth) {
		ExternalAuthService old = authServices.putIfAbsent(auth.getName(), auth);
		if (old != null)
			throw new IllegalStateException("duplicated logdb auth service name: " + auth.getName());
	}

	@Override
	public void unregisterAuthService(ExternalAuthService auth) {
		authServices.remove(auth.getName(), auth);
	}

	@Override
	public void addListener(AccountEventListener listener) {
		accountListeners.add(listener);
	}

	@Override
	public void removeListener(AccountEventListener listener) {
		accountListeners.remove(listener);
	}

	@Override
	public void addListener(SessionEventListener listener) {
		sessionListeners.add(listener);
	}

	@Override
	public void removeListener(SessionEventListener listener) {
		sessionListeners.remove(listener);
	}

	@Override
	public void onCreate(TableSchema schema) {
	}

	@Override
	public void onAlter(TableSchema oldSchema, TableSchema newSchema) {
	}

	@Override
	public void onDrop(TableSchema schema) {
		String tableName = schema.getName();

		// remove all granted permissions for this table
		for (Account account : localAccounts.values()) {
			if (account.getReadableTables().contains(tableName)) {
				ArrayList<String> copy = new ArrayList<String>(account.getReadableTables());
				copy.remove(tableName);
				account.setReadableTables(copy);
				updateAccount(account);
			}
		}

		for (SecurityGroup group : securityGroups.values()) {
			if (group.getReadableTables().contains(tableName)) {
				group.getReadableTables().remove(tableName);
				updateSecurityGroup(null, group);
			}
		}
	}

	@Override
	public String getInstanceGuid() {
		return instanceGuid;
	}

	@Override
	public void setInstanceGuid(String guid) {
		ConfigDatabase db = conf.ensureDatabase(DB_NAME);
		ConfigCollection col = db.ensureCollection("global_config");
		Config c = col.findOne(null);

		if (c != null) {
			@SuppressWarnings("unchecked")
			Map<String, Object> doc = (Map<String, Object>) c.getDocument();
			doc.put("instance_guid", guid);
			c.setDocument(doc);
			c.update();
		} else {
			Map<String, Object> doc = new HashMap<String, Object>();
			doc.put("instance_guid", guid);
			col.add(doc);
		}
		this.instanceGuid = guid;
	}

}
