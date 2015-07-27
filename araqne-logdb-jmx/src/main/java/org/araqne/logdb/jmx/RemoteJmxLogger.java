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
package org.araqne.logdb.jmx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXServiceURL;

import org.araqne.log.api.AbstractLogger;
import org.araqne.log.api.Log;
import org.araqne.log.api.LoggerFactory;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.Reconfigurable;
import org.araqne.log.api.SimpleLog;

public class RemoteJmxLogger extends AbstractLogger implements Reconfigurable {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(RemoteJmxLogger.class);

	private String user;
	private String password;
	private JMXServiceURL url;
	private ObjectName objName;
	private String[] attrNames;

	public RemoteJmxLogger(LoggerSpecification spec, LoggerFactory factory) throws IOException {
		super(spec, factory);
		init(spec.getConfig());
	}

	private void init(Map<String, String> c) throws IOException {
		String[] attrNames = null;
		String s = c.get("attr_names");
		if (s != null) {
			ArrayList<String> l = new ArrayList<String>();
			for (String t : s.split(",")) {
				t = t.trim();
				if (!t.isEmpty())
					l.add(t);
			}
			attrNames = l.toArray(new String[0]);
		}

		ObjectName objName;
		try {
			objName = new ObjectName(c.get("obj_name"));
		} catch (Exception e) {
			throw new IOException("invalid jmx object name: " + c.get("obj_name"), e);
		}

		this.url = JmxHelper.getURL(c.get("host"), Integer.parseInt(c.get("port")));
		this.user = c.get("user");
		this.password = c.get("password");
		this.attrNames = attrNames;
		this.objName = objName;
	}

	@Override
	public void onConfigChange(Map<String, String> oldConfigs, Map<String, String> newConfigs) {
		try {
			init(newConfigs);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	protected void runOnce() {
		JMXConnector jmxConnector = null;
		try {
			jmxConnector = JmxHelper.connect(url, user, password);
			MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();
			String[] names = attrNames;
			if (names == null)
				names = JmxHelper.getAttributeNames(mbeanConn, objName);

			AttributeList attrs = mbeanConn.getAttributes(objName, names);
			Log log = buildLog(attrs);
			write(log);
			setTemporaryFailure(null);
		} catch (Throwable t) {
			setTemporaryFailure(t);
			slog.error("araqne log api: remote jmx failed, logger " + getFullName(), t);
		} finally {
			if (jmxConnector != null) {
				try {
					jmxConnector.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private Log buildLog(AttributeList attrs) {
		Map<String, Object> data = new HashMap<String, Object>();
		for (Attribute attr : attrs.asList()) {
			Object value = attr.getValue();
			if (value instanceof Boolean || value instanceof Long || value instanceof Integer || value instanceof Short
					|| value instanceof Float || value instanceof Double || value instanceof String) {
				data.put(attr.getName(), value);
			} else if (value != null) {
				data.put(attr.getName(), value.toString());
			} else {
				data.put(attr.getName(), null);
			}
		}
		return new SimpleLog(new Date(), getFullName(), data);
	}
}
