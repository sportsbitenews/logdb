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
package org.araqne.logdb.jms.query;

import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.QueryTask.TaskStatus;
import org.araqne.logdb.jms.JmsProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsRecvCommand extends DriverQueryCommand {
	private final Logger slog = LoggerFactory.getLogger(JmsRecvCommand.class);
	private JmsProfile profile;
	private Integer maxCount;
	private TimeSpan window;

	public JmsRecvCommand(JmsProfile profile, Integer maxCount, TimeSpan window) {
		this.profile = profile;
		this.maxCount = maxCount;
		this.window = window;
	}

	@Override
	public String getName() {
		return "jmsrecv";
	}

	@Override
	public void run() {
		Connection connection = null;
		Session session = null;
		MessageConsumer consumer = null;
		try {
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(profile.getEndpoint());
			connection = connectionFactory.createConnection();
			connection.start();

			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createQueue(profile.getQueueName());
			consumer = session.createConsumer(destination);

			int count = 0;

			long begin = System.currentTimeMillis();
			long limitTime = Long.MAX_VALUE;
			if (window != null)
				limitTime = begin + window.unit.getMillis() * window.amount;

			while (true) {
				if (task.getStatus() == TaskStatus.CANCELED)
					break;

				if (maxCount != null && count >= maxCount)
					break;

				long now = System.currentTimeMillis();
				if (limitTime <= now)
					break;

				Message message = consumer.receive(100);
				if (message == null)
					continue;

				Row row = parse(message);
				if (row != null)
					pushPipe(row);

				count++;
			}
		} catch (JMSException e) {
			slog.error("araqne logdb jms: recv failure, profile=" + profile.getName(), e);
		} finally {
			closeAll(connection, session, consumer);
		}
	}

	private Row parse(Message message) throws JMSException {
		if (message instanceof TextMessage) {
			TextMessage msg = (TextMessage) message;
			String text = msg.getText();
			Date date = msg.getJMSTimestamp() == 0 ? new Date() : new Date(msg.getJMSTimestamp());

			Map<String, Object> m = new HashMap<String, Object>();
			m.put("_time", date);
			m.put("_msg_id", msg.getJMSMessageID());
			m.put("line", text);

			return new Row(m);
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

			return new Row(m);
		}

		return null;
	}

	private void closeAll(Connection connection, Session session, MessageConsumer consumer) {
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
