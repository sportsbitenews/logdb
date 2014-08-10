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

import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.QueryTask.TaskStatus;
import org.araqne.logdb.Row;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.jms.JmsProfile;
import org.araqne.logdb.jms.impl.JmsHelper;
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

				Map<String, Object> row = JmsHelper.parse(message);
				if (row != null)
					pushPipe(new Row(row));

				count++;
			}
		} catch (JMSException e) {
			slog.error("araqne logdb jms: recv failure, profile=" + profile.getName(), e);
		} finally {
			JmsHelper.closeAll(connection, session, consumer);
		}
	}
}
