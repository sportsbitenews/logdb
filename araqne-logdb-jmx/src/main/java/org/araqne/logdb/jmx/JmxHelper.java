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
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class JmxHelper {
	private JmxHelper() {
	}

	public static JMXServiceURL getURL(String host, int port) throws MalformedURLException {
		return new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi");
	}

	public static JMXConnector connect(JMXServiceURL url, String user, String password) throws IOException {
		String[] credentials = new String[] { user, password };
		Map<String, String[]> props = new HashMap<String, String[]>();
		props.put("jmx.remote.credentials", credentials);

		return JMXConnectorFactory.connect(url, props);
	}

	public static String[] getAttributeNames(MBeanServerConnection conn, ObjectName objName) throws InstanceNotFoundException,
			IntrospectionException, ReflectionException, IOException {
		MBeanInfo bean = conn.getMBeanInfo(objName);
		String[] names = new String[bean.getAttributes().length];
		int i = 0;
		for (MBeanAttributeInfo attr : bean.getAttributes()) {
			System.out.println(attr);
			names[i++] = attr.getName();
		}
		return names;
	}
}
