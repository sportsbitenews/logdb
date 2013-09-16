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
package org.araqne.logdb.jmx.impl;

import java.io.IOException;
import java.util.Set;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXServiceURL;

import org.araqne.api.Script;
import org.araqne.api.ScriptArgument;
import org.araqne.api.ScriptContext;
import org.araqne.api.ScriptUsage;
import org.araqne.logdb.jmx.JmxHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmxScript implements Script {
	private final Logger logger = LoggerFactory.getLogger(JmxScript.class);
	private ScriptContext context;

	@Override
	public void setScriptContext(ScriptContext context) {
		this.context = context;
	}

	@ScriptUsage(description = "query JMX object names", arguments = {
			@ScriptArgument(name = "remote ip endpoint", type = "string", description = "host:port"),
			@ScriptArgument(name = "user", type = "string", description = "user"),
			@ScriptArgument(name = "password", type = "string", description = "password")
	})
	public void objectNames(String[] args) {
		String host = args[0].split(":")[0];
		int port = Integer.valueOf(args[0].split(":")[1]);
		String user = args[1];
		String password = args[2];

		JMXConnector jmxConnector = null;
		try {
			context.println("Connecting to host...");
			JMXServiceURL url = JmxHelper.getURL(host, port);
			jmxConnector = JmxHelper.connect(url, user, password);
			MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();

			Set<ObjectName> beanSet = mbeanConn.queryNames(null, null);
			for (ObjectName name : beanSet)
				context.println(name);
		} catch (Throwable t) {
			context.println("jmx query failed: " + t.getMessage());
			logger.error("araqne logdb jmx: jmx query failed", t);
		} finally {
			if (jmxConnector != null) {
				try {
					jmxConnector.close();
				} catch (IOException e) {
				}
			}
		}
	}

	@ScriptUsage(description = "query JMX attributes", arguments = {
			@ScriptArgument(name = "remote ip endpoint", type = "string", description = "host:port"),
			@ScriptArgument(name = "user", type = "string", description = "user"),
			@ScriptArgument(name = "password", type = "string", description = "password"),
			@ScriptArgument(name = "object name", type = "string", description = "JMX object name")
	})
	public void attrs(String[] args) {

		String host = args[0].split(":")[0];
		int port = Integer.valueOf(args[0].split(":")[1]);
		String user = args[1];
		String password = args[2];
		String objName = args[3];

		JMXConnector jmxConnector = null;
		try {
			context.println("Connecting to host...");
			JMXServiceURL url = JmxHelper.getURL(host, port);
			jmxConnector = JmxHelper.connect(url, user, password);
			MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();

			ObjectName objectName = new ObjectName(objName);
			String[] names = JmxHelper.getAttributeNames(mbeanConn, objectName);

			AttributeList attrs = mbeanConn.getAttributes(objectName, names);
			for (Attribute attr : attrs.asList()) {
				context.println(attr);
			}

		} catch (Throwable t) {
			context.println("jmx query failed: " + t.getMessage());
			logger.error("araqne logdb jmx: jmx query failed", t);
		} finally {
			if (jmxConnector != null) {
				try {
					jmxConnector.close();
				} catch (IOException e) {
				}
			}
		}

	}

}
