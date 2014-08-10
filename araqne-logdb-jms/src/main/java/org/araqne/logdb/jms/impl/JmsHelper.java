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
package org.araqne.logdb.jms.impl;

import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsHelper {

	public static Map<String, Object> parse(Message message) throws JMSException {
		if (message instanceof TextMessage) {
			TextMessage msg = (TextMessage) message;
			String text = msg.getText();
			Date date = msg.getJMSTimestamp() == 0 ? new Date() : new Date(msg.getJMSTimestamp());

			Map<String, Object> m = new HashMap<String, Object>();
			m.put("_time", date);
			m.put("_msg_id", msg.getJMSMessageID());
			m.put("line", text);

			return m;
		} else if (message instanceof MapMessage) {
			MapMessage msg = (MapMessage) message;
			Date date = msg.getJMSTimestamp() == 0 ? new Date() : new Date(msg.getJMSTimestamp());

			Map<String, Object> m = new HashMap<String, Object>();
			m.put("_time", date);
			m.put("_msg_id", msg.getJMSMessageID());

			@SuppressWarnings("unchecked")
			Enumeration<String> e = msg.getPropertyNames();
			while (e.hasMoreElements()) {
				String key = e.nextElement();
				Object val = msg.getObjectProperty(key);
				m.put(key, val);
			}

			return m;
		}

		return null;
	}

	public static void closeAll(Connection connection, Session session, MessageConsumer consumer) {
		Logger slog = LoggerFactory.getLogger(JmsHelper.class);

		try {
			if (consumer != null)
				consumer.close();
		} catch (JMSException e) {
			slog.trace("araqne logdb jms: cannot close consumer", e);
		}

		try {
			if (session != null)
				session.close();
		} catch (JMSException e) {
			slog.trace("araqne logdb jms: cannot close session", e);
		}

		try {
			if (connection != null)
				connection.close();
		} catch (JMSException e) {
			slog.trace("araqne logdb jms: cannot close connection", e);
		}
	}

}
