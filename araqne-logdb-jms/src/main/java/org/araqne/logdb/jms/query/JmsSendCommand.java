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

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.jms.JmsProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsSendCommand extends QueryCommand {
	private final Logger slog = LoggerFactory.getLogger(JmsSendCommand.class);
	private JmsProfile profile;

	private Connection connection = null;
	private Session session = null;
	private MessageProducer producer = null;

	public JmsSendCommand(JmsProfile profile) {
		this.profile = profile;
	}

	@Override
	public String getName() {
		return "jmssend";
	}

	@Override
	public void onStart() {
		try {
			if (slog.isTraceEnabled())
				slog.trace("araqne logdb jms: starting jmssend, profile [{}]", profile.getName());

			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(profile.getEndpoint());
			connection = connectionFactory.createConnection();
			connection.start();

			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createQueue(profile.getQueueName());
			producer = session.createProducer(destination);
		} catch (JMSException e) {
			throw new IllegalStateException("jms connect failure", e);
		}
	}

	@Override
	public void onClose(QueryStopReason reason) {
		if (slog.isTraceEnabled())
			slog.trace("araqne logdb jms: closing jmssend, profile [{}]", profile.getName());

		closeAll();
	}

	@Override
	public void onPush(Row row) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		MapMessage msg;
		try {
			msg = session.createMapMessage();

			for (String key : row.map().keySet()) {
				Object val = row.get(key);
				if (val instanceof Date) {
					val = df.format(val);
				} else if (val instanceof InetAddress) {
					val = val.toString();
				}

				msg.setObjectProperty(key, val);
			}

			producer.send(msg);
		} catch (JMSException e) {
			slog.trace("araqne logdb jms: send failure, profile=" + profile.getName(), e);
		}

		pushPipe(row);
	}

	private void closeAll() {
		try {
			if (producer != null) {
				producer.close();
				producer = null;
			}
		} catch (JMSException e) {
			slog.trace("araqne logdb jms: cannot close consumer", e);
		}

		try {
			if (session != null) {
				session.close();
				session = null;
			}
		} catch (JMSException e) {
			slog.trace("araqne logdb jms: cannot close session", e);
		}

		try {
			if (connection != null) {
				connection.close();
				connection = null;
			}
		} catch (JMSException e) {
			slog.trace("araqne logdb jms: cannot close connection", e);
		}
	}
}
