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
package org.araqne.logdb.jms.logger;

import java.util.Date;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.araqne.log.api.AbstractLogger;
import org.araqne.log.api.LoggerFactory;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.LoggerStopReason;
import org.araqne.log.api.SimpleLog;
import org.araqne.logdb.jms.JmsProfile;
import org.araqne.logdb.jms.JmsProfileRegistry;
import org.araqne.logdb.jms.impl.JmsHelper;

public class JmsLogger extends AbstractLogger implements MessageListener {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(JmsLogger.class);
	private Connection connection;
	private Session session;
	private MessageConsumer consumer;
	private JmsProfileRegistry profileRegistry;
	private String profileName;

	public JmsLogger(LoggerSpecification spec, LoggerFactory factory, JmsProfileRegistry profileRegistry) {
		super(spec, factory);

		this.profileRegistry = profileRegistry;
		this.profileName = getConfigs().get("jms_profile");
	}

	@Override
	protected void onStart() {
		JmsProfile profile = profileRegistry.getProfile(profileName);
		if (profile == null)
			throw new IllegalStateException("jms profile not found: " + profileName);

		try {
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(profile.getEndpoint());
			connection = connectionFactory.createConnection();
			connection.start();

			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createQueue(profile.getQueueName());
			consumer = session.createConsumer(destination);
			consumer.setMessageListener(this);
		} catch (JMSException e) {
			JmsHelper.closeAll(connection, session, consumer);
			connection = null;
			session = null;
			consumer = null;
			throw new IllegalStateException("jms connect failure, logger=" + getFullName() + ", profile=" + profileName, e);
		}
	}

	@Override
	protected void onStop(LoggerStopReason reason) {
		JmsHelper.closeAll(connection, session, consumer);
	}

	@Override
	public boolean isPassive() {
		return true;
	}

	@Override
	public void onMessage(Message message) {
		try {
			Map<String, Object> row = JmsHelper.parse(message);
			if (row != null) {
				Date date = message.getJMSTimestamp() == 0 ? new Date() : new Date(message.getJMSTimestamp());
				row.remove("_time");
				write(new SimpleLog(date, getFullName(), row));
			}
		} catch (JMSException e) {
			slog.trace("araqne logdb jms: logger [" + getFullName() + "] recv failure", e);
		}
	}

	@Override
	protected void runOnce() {
	}
}
