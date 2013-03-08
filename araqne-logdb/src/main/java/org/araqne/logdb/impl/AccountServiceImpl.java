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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.api.PrimitiveConverter;
import org.araqne.confdb.Config;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigService;
import org.araqne.confdb.Predicates;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.Permission;
import org.araqne.logdb.Session;
import org.araqne.logstorage.LogTableEventListener;
import org.araqne.logstorage.LogTableRegistry;

@Component(name = "logdb-account")
@Provides(specifications = { AccountService.class })
public class AccountServiceImpl implements AccountService, LogTableEventListener {
	private static final String DB_NAME = "araqne-logdb";
	private static final String MASTER_ACCOUNT = "araqne";
	private static final char[] SALT_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray();

	@Requires
	private ConfigService conf;

	@Requires
	private LogTableRegistry tableRegistry;

	private ConcurrentMap<String, Session> sessions;
	private ConcurrentMap<String, Account> accounts;

	public AccountServiceImpl() {
		sessions = new ConcurrentHashMap<String, Session>();
		accounts = new ConcurrentHashMap<String, Account>();
	}

	@Validate
	public void start() {
		tableRegistry.addListener(this);
		sessions.clear();
		accounts.clear();

		// load accounts
		ConfigDatabase db = conf.ensureDatabase(DB_NAME);
		for (Account account : db.findAll(Account.class).getDocuments(Account.class)) {
			accounts.put(account.getLoginName(), account);
		}

		// generate default 'araqne' account if not exists
		if (!accounts.containsKey(MASTER_ACCOUNT)) {
			String salt = randomSalt(10);
			Account account = new Account(MASTER_ACCOUNT, salt, Sha1.hash(salt));
			db.add(account);
			accounts.put(MASTER_ACCOUNT, account);
		}
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
	public Set<String> getAccountNames() {
		return new HashSet<String>(accounts.keySet());
	}

	@Override
	public boolean verifyPassword(String loginName, String password) {
		verifyNotNull(loginName, "login name");
		verifyNotNull(password, "password");

		Account account = accounts.get(loginName);
		if (account == null)
			throw new IllegalStateException("account not found");

		// salted hash
		String hash = account.getPassword();
		String salt = account.getSalt();

		return hash.equals(Sha1.hash(password + salt));
	}

	@Override
	public Session login(String loginName, String hash, String nonce) {
		verifyNotNull(loginName, "login name");
		verifyNotNull(hash, "hash");
		verifyNotNull(nonce, "nonce");

		Account account = accounts.get(loginName);
		if (account == null)
			throw new IllegalStateException("account-not-found");

		String password = account.getPassword();
		if (!hash.equals(Sha1.hash(password + nonce))) {
			throw new IllegalStateException("invalid-password");
		}

		return registerSession(loginName);
	}

	@Override
	public Session login(String loginName, String password) {
		if (!verifyPassword(loginName, password))
			throw new IllegalStateException("invalid password");

		return registerSession(loginName);
	}

	private Session registerSession(String loginName) {
		String guid = UUID.randomUUID().toString();
		Session session = new SessionImpl(guid, loginName);
		sessions.put(guid, session);
		return session;
	}

	@Override
	public void logout(Session session) {
		if (!sessions.remove(session.getGuid(), session))
			throw new IllegalStateException("session not found: " + session.getGuid());
	}

	@Override
	public void createAccount(Session session, String loginName, String password) {
		verifyNotNull(session, "session");
		verifyNotNull(loginName, "login name");
		verifyNotNull(password, "password");

		if (accounts.containsKey(loginName))
			throw new IllegalStateException("duplicated login name");

		if (!sessions.containsKey(session.getGuid()))
			throw new IllegalStateException("invalid session");

		if (!session.getLoginName().equals(MASTER_ACCOUNT))
			throw new IllegalStateException("no permission");

		// check database
		ConfigDatabase db = conf.ensureDatabase(DB_NAME);
		Config c = db.findOne(Account.class, Predicates.field("login_name", loginName));
		if (c != null)
			throw new IllegalStateException("duplicated login name");

		String salt = randomSalt(10);
		String hash = Sha1.hash(password + salt);
		Account account = new Account(loginName, salt, hash);

		Account old = accounts.putIfAbsent(account.getLoginName(), account);
		if (old != null)
			throw new IllegalStateException("duplicated login name");

		db.add(account);
	}

	@Override
	public void changePassword(Session session, String loginName, String password) {
		verifyNotNull(session, "session");
		verifyNotNull(loginName, "login name");
		verifyNotNull(password, "password");

		if (!accounts.containsKey(loginName))
			throw new IllegalStateException("account not found");

		if (!sessions.containsKey(session.getGuid()))
			throw new IllegalStateException("invalid session");

		// check if owner or master
		if (!loginName.equals(session.getLoginName()) && !session.getLoginName().equals(MASTER_ACCOUNT))
			throw new IllegalStateException("no permission");

		Account account = accounts.get(loginName);
		String hash = Sha1.hash(password + account.getSalt());
		account.setPassword(hash);

		ConfigDatabase db = conf.ensureDatabase(DB_NAME);
		Config c = db.findOne(Account.class, Predicates.field("login_name", loginName));
		if (c != null) {
			c.setDocument(PrimitiveConverter.serialize(account));
			c.update();
		} else {
			db.add(account);
		}
	}

	@Override
	public void removeAccount(Session session, String loginName) {
		verifyNotNull(session, "session");
		verifyNotNull(loginName, "login name");

		if (!accounts.containsKey(loginName))
			throw new IllegalStateException("account not found");

		if (!sessions.containsKey(session.getGuid()))
			throw new IllegalStateException("invalid session");

		// master admin only
		if (!session.getLoginName().equals(MASTER_ACCOUNT))
			throw new IllegalStateException("no permission");

		accounts.remove(loginName);

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
	}

	@Override
	public boolean checkPermission(Session session, String tableName, Permission permission) {
		verifyNotNull(session, "session");
		verifyNotNull(tableName, "table name");
		verifyNotNull(permission, "permission");

		if (permission != Permission.READ)
			throw new UnsupportedOperationException();

		Account account = accounts.get(session.getLoginName());
		if (account == null)
			throw new IllegalStateException("account not found");

		if (session.getLoginName().equals("araqne"))
			return true;

		return account.getReadableTables().contains(tableName);
	}

	@Override
	public void grantPrivilege(Session session, String loginName, String tableName, Permission... permissions) {
		verifyNotNull(session, "session");
		verifyNotNull(loginName, "login name");
		verifyNotNull(tableName, "table name");

		if (permissions.length == 0)
			return;

		if (!accounts.containsKey(loginName))
			throw new IllegalStateException("account not found");

		if (!sessions.containsKey(session.getGuid()))
			throw new IllegalStateException("invalid session");

		// master admin only
		if (!session.getLoginName().equals(MASTER_ACCOUNT))
			throw new IllegalStateException("no permission");

		if (!tableRegistry.exists(tableName))
			throw new IllegalStateException("table not found");

		Account account = accounts.get(loginName);
		if (account.getReadableTables().contains(tableName))
			return;

		account.getReadableTables().add(tableName);

		ConfigDatabase db = conf.ensureDatabase(DB_NAME);
		Config c = db.findOne(Account.class, Predicates.field("login_name", loginName));
		if (c != null) {
			c.setDocument(PrimitiveConverter.serialize(account));
			c.update();
		} else {
			db.add(account);
		}
	}

	@Override
	public void revokePrivilege(Session session, String loginName, String tableName, Permission... permissions) {
		verifyNotNull(session, "session");
		verifyNotNull(loginName, "login name");
		verifyNotNull(tableName, "table name");

		if (permissions.length == 0)
			return;

		if (!accounts.containsKey(loginName))
			throw new IllegalStateException("account not found");

		if (!sessions.containsKey(session.getGuid()))
			throw new IllegalStateException("invalid session");

		// master admin only
		if (!session.getLoginName().equals(MASTER_ACCOUNT))
			throw new IllegalStateException("no permission");

		if (!tableRegistry.exists(tableName))
			throw new IllegalStateException("table not found");

		Account account = accounts.get(loginName);
		if (!account.getReadableTables().contains(tableName))
			return;

		account.getReadableTables().remove(tableName);

		ConfigDatabase db = conf.ensureDatabase(DB_NAME);
		Config c = db.findOne(Account.class, Predicates.field("login_name", loginName));
		if (c != null) {
			c.setDocument(PrimitiveConverter.serialize(account));
			c.update();
		} else {
			db.add(account);
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
	public void onCreate(String tableName, Map<String, String> tableMetadata) {
	}

	@Override
	public void onDrop(String tableName) {
		// remove all granted permissions for this table
		for (Account account : accounts.values()) {
			if (account.getReadableTables().contains(tableName)) {
				ArrayList<String> copy = new ArrayList<String>(account.getReadableTables());
				copy.remove(tableName);
				account.setReadableTables(copy);

				ConfigDatabase db = conf.ensureDatabase(DB_NAME);
				Config c = db.findOne(Account.class, Predicates.field("login_name", account.getLoginName()));
				if (c != null) {
					c.setDocument(PrimitiveConverter.serialize(account));
					c.update();
				} else {
					db.add(account);
				}
			}
		}
	}
}
