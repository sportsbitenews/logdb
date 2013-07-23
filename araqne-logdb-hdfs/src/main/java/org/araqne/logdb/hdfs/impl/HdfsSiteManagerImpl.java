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
package org.araqne.logdb.hdfs.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.hadoop.fs.FileSystem;
import org.araqne.confdb.Config;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigService;
import org.araqne.confdb.Predicates;
import org.araqne.logdb.hdfs.HdfsSite;
import org.araqne.logdb.hdfs.HdfsSiteManager;

@Component(name = "logdb-hdfs-site-manager")
@Provides
public class HdfsSiteManagerImpl implements HdfsSiteManager {

	@Requires
	private ConfigService conf;

	@Invalidate
	public void stop() {
		try {
			FileSystem.closeAll();
		} catch (IOException e) {
		}
	}

	@Override
	public List<HdfsSite> getSites() {
		ConfigDatabase db = getDatabase();
		return new ArrayList<HdfsSite>(db.findAll(HdfsSite.class).getDocuments(HdfsSite.class));
	}

	@Override
	public HdfsSite getSite(String name) {
		ConfigDatabase db = getDatabase();
		Config c = db.findOne(HdfsSite.class, Predicates.field("name", name));
		if (c == null)
			return null;

		return c.getDocument(HdfsSite.class);
	}

	@Override
	public void addSite(HdfsSite site) {
		ConfigDatabase db = getDatabase();
		Config c = db.findOne(HdfsSite.class, Predicates.field("name", site.getName()));
		if (c != null)
			throw new IllegalStateException("duplicated hdfs site name: " + site.getName());

		db.add(site);
	}

	@Override
	public void removeSite(String name) {
		ConfigDatabase db = getDatabase();
		Config c = db.findOne(HdfsSite.class, Predicates.field("name", name));
		if (c == null)
			throw new IllegalStateException("hdfs site not found: " + name);

		c.remove();
	}

	private ConfigDatabase getDatabase() {
		return conf.ensureDatabase("araqne-logdb-hdfs");
	}
}
