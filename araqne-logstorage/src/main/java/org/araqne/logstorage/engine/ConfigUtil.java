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
package org.araqne.logstorage.engine;

import java.util.HashMap;
import java.util.Map;

import org.araqne.confdb.Config;
import org.araqne.confdb.ConfigCollection;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigIterator;
import org.araqne.confdb.ConfigService;

public class ConfigUtil {
	public static String get(ConfigService conf, Constants key) {
		ConfigDatabase db = conf.ensureDatabase("araqne-logstorage");
		ConfigCollection col = db.ensureCollection("global_settings");
		Config c = getGlobalConfig(col);
		if (c == null)
			return null;

		@SuppressWarnings("unchecked")
		Map<String, Object> m = (Map<String, Object>) c.getDocument();
		return (String) m.get(key.getName());
	}

	@SuppressWarnings("unchecked")
	public static void set(ConfigService conf, Constants key, String value) {
		ConfigDatabase db = conf.ensureDatabase("araqne-logstorage");
		ConfigCollection col = db.ensureCollection("global_settings");

		Config c = getGlobalConfig(col);

		Map<String, Object> doc = new HashMap<String, Object>();
		if (c != null)
			doc = (Map<String, Object>) c.getDocument();

		if (value != null) {
			doc.put(key.getName(), value);
		} else {
			doc.remove(key.getName());
		}

		if (c == null)
			col.add(doc);
		else
			col.update(c);
	}

	private static Config getGlobalConfig(ConfigCollection col) {
		ConfigIterator it = col.findAll();
		try {
			if (!it.hasNext())
				return null;

			return it.next();
		} finally {
			it.close();
		}
	}
}
