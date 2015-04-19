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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.confdb.Config;
import org.araqne.confdb.ConfigCollection;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigService;
import org.araqne.confdb.Predicates;
import org.araqne.logdb.CsvLookupRegistry;
import org.araqne.logdb.LookupHandler;
import org.araqne.logdb.LookupHandlerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVReader;

/**
 * If you need per-user csv lookup management, implement or use other service
 * component. This csv lookup service provides only global configuration.
 * 
 * @author xeraph
 * 
 */
@Component(name = "logdb-csv-lookup-registry")
@Provides
public class CsvLookupRegistryImpl implements CsvLookupRegistry {
	private final Logger logger = LoggerFactory.getLogger(CsvLookupRegistryImpl.class);

	@Requires
	private ConfigService conf;

	@Requires
	private LookupHandlerRegistry lookup;

	@Validate
	public void start() {
		for (File f : getCsvFiles()) {
			String name = getLookupName(f);
			try {
				logger.debug("araqne logdb: adding csv lookup handler [{}]", name);
				lookup.addLookupHandler(name, new CsvLookupHandler(f));
			} catch (Throwable t) {
				logger.error("araqne logdb: cannot add csv lookup handler - " + name, t);
			}
		}
	}

	@Invalidate
	public void stop() {
		for (File f : getCsvFiles()) {
			String name = getLookupName(f);
			try {
				logger.debug("araqne logdb: removing csv lookup handler [{}]", name);
				if (lookup != null)
					lookup.removeLookupHandler(name);
			} catch (Throwable t) {
				logger.error("araqne logdb: cannot remove csv lookup handler - " + name, t);
			}
		}
	}

	@Override
	public Set<File> getCsvFiles() {
		ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
		ConfigCollection col = db.ensureCollection("csv_lookups");

		Set<File> files = new HashSet<File>();
		for (Object o : col.findAll().getDocuments()) {
			@SuppressWarnings("unchecked")
			Map<String, Object> m = (Map<String, Object>) o;
			files.add(new File((String) m.get("path")));
		}

		return files;
	}

	@Override
	public void loadCsvFile(File f) throws IOException {
		if (f == null)
			throw new IllegalArgumentException("csv path should not be null");

		if (!f.exists())
			throw new IllegalStateException("csv path doesn't exist: " + f.getAbsolutePath());

		if (!f.isFile())
			throw new IllegalStateException("csv path is not file: " + f.getAbsolutePath());

		if (!f.canRead())
			throw new IllegalStateException("cannot read csv file, check read permission: " + f.getAbsolutePath());

		ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
		ConfigCollection col = db.ensureCollection("csv_lookups");

		// check duplicate
		Config c = col.findOne(Predicates.field("path", f.getAbsolutePath()));
		if (c != null)
			throw new IllegalStateException("csv path already exists: " + f.getAbsolutePath());

		// add to lookup handler service
		lookup.addLookupHandler(getLookupName(f), new CsvLookupHandler(f));

		// add
		Map<String, Object> doc = new HashMap<String, Object>();
		doc.put("path", f.getAbsolutePath());
		col.add(doc);
	}

	@Override
	public void unloadCsvFile(File f) {
		if (f == null)
			throw new IllegalArgumentException("csv path should not be null");

		ConfigDatabase db = conf.ensureDatabase("araqne-logdb");
		ConfigCollection col = db.ensureCollection("csv_lookups");

		// check existence
		Config c = col.findOne(Predicates.field("path", f.getAbsolutePath()));
		if (c == null)
			throw new IllegalStateException("not registered path: " + f.getAbsolutePath());

		c.remove();

		// remove from lookup handler service
		lookup.removeLookupHandler(getLookupName(f));
	}

	private String getLookupName(File f) {
		return "csv$" + f.getName();
	}

	private class CsvLookupHandler implements LookupHandler {
		private String keyFieldName;
		private ArrayList<String> valueFieldNames;
		private Map<String, Map<String, String>> mappings = new HashMap<String, Map<String, String>>();

		public CsvLookupHandler(File f) throws IOException {
			CSVReader reader = null;
			FileInputStream is = null;

			try {
				int skipBytes = getBomLength(f);

				is = new FileInputStream(f);
				is.skip(skipBytes);

				reader = new CSVReader(new InputStreamReader(is, "utf-8"));

				String[] nextLine = reader.readNext();
				if (nextLine == null)
					throw new IllegalStateException("header columns not found");

				if (nextLine.length < 2)
					throw new IllegalStateException("not enough columns (should be 2 or more)");

				keyFieldName = nextLine[0];
				valueFieldNames = new ArrayList<String>(nextLine.length - 1);
				for (int i = 1; i < nextLine.length; i++)
					valueFieldNames.add(nextLine[i]);

				if (logger.isDebugEnabled())
					logger.debug("araqne logdb: key field [{}] value fields [{}]", keyFieldName, valueFieldNames);

				while ((nextLine = reader.readNext()) != null) {
					Map<String, String> values = new HashMap<String, String>();
					for (int i = 1; i < nextLine.length; i++) {
						String valueFieldName = valueFieldNames.get(i - 1);
						String value = nextLine[i];
						values.put(valueFieldName, value);
					}

					mappings.put(nextLine[0], values);
				}
			} finally {
				if (reader != null)
					reader.close();
			}
		}

		// support utf8 BOM only
		private int getBomLength(File f) throws IOException {
			if (f.length() < 3)
				return 0;

			byte[] buf = new byte[3];
			byte[] bom = new byte[] { (byte) 0xef, (byte) 0xbb, (byte) 0xbf };
			RandomAccessFile raf = null;
			try {
				raf = new RandomAccessFile(f, "r");
				raf.readFully(buf);

				if (Arrays.equals(buf, bom))
					return 3;
			} finally {
				if (raf != null) {
					try {
						raf.close();
					} catch (IOException e) {
					}
				}
			}

			return 0;
		}

		@Override
		public Object lookup(String srcField, String dstField, Object srcValue) {
			if (srcValue == null)
				return null;
			
			if (!valueFieldNames.contains(dstField))
				return null;

			Map<String, String> valueMappings = mappings.get(srcValue.toString());
			if (valueMappings == null)
				return null;

			return valueMappings.get(dstField);
		}
	}
}
