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
package org.araqne.log.api.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.log.api.LogTransformerFactory;
import org.araqne.log.api.LogTransformerFactoryRegistry;
import org.araqne.log.api.LogTransformerFactoryRegistryEventListener;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;

@Component(name = "log-transformer-factory-registry")
@Provides
public class LogTransformerFactoryRegistryImpl implements LogTransformerFactoryRegistry {
	private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LogTransformerFactoryRegistryImpl.class);
	private BundleContext bc;
	private Tracker tracker;
	private ConcurrentMap<String, LogTransformerFactory> factoryMap;
	private CopyOnWriteArraySet<LogTransformerFactoryRegistryEventListener> listeners;

	public LogTransformerFactoryRegistryImpl(BundleContext bc) {
		this.bc = bc;
		this.tracker = new Tracker();
	}

	@Validate
	public void start() {
		listeners = new CopyOnWriteArraySet<LogTransformerFactoryRegistryEventListener>();
		factoryMap = new ConcurrentHashMap<String, LogTransformerFactory>();
		tracker.open();
	}

	@Invalidate
	public void stop() {
		listeners.clear();
		tracker.close();
	}

	@Override
	public void register(LogTransformerFactory factory) {
		LogTransformerFactory old = factoryMap.putIfAbsent(factory.getName(), factory);
		if (old != null)
			throw new IllegalStateException("duplicated transformer factory mapping: " + factory.getName());

		for (LogTransformerFactoryRegistryEventListener el : listeners) {
			try {
				el.factoryAdded(factory);
			} catch (Throwable t) {
				logger.warn("araqne log api: transformer factory event listener should not throw any exception", t);
			}
		}

		logger.info("araqne log api: new transformer factory [{}] added", factory.getName());
	}

	@Override
	public void unregister(LogTransformerFactory factory) {
		if (!factoryMap.remove(factory.getName(), factory))
			throw new IllegalStateException("transformer factory not found: " + factory.getName());

		for (LogTransformerFactoryRegistryEventListener el : listeners) {
			try {
				el.factoryRemoved(factory);
			} catch (Throwable t) {
				logger.warn("araqne log api: transformer factory event listener should not throw any exception", t);
			}
		}

		logger.info("araqne log api: transformer factory [{}] removed", factory.getName());
	}

	@Override
	public List<LogTransformerFactory> getFactories() {
		return new ArrayList<LogTransformerFactory>(factoryMap.values());
	}

	@Override
	public LogTransformerFactory getFactory(String name) {
		return factoryMap.get(name);
	}

	@Override
	public void addListener(LogTransformerFactoryRegistryEventListener listener) {
		listeners.add(listener);
	}

	@Override
	public void removeListener(LogTransformerFactoryRegistryEventListener listener) {
		listeners.remove(listener);
	}

	private class Tracker extends ServiceTracker {
		public Tracker() {
			super(bc, LogTransformerFactory.class.getName(), null);
		}

		@Override
		public Object addingService(ServiceReference reference) {
			LogTransformerFactory f = (LogTransformerFactory) super.addingService(reference);
			register(f);
			return f;
		}

		@Override
		public void removedService(ServiceReference reference, Object service) {
			unregister((LogTransformerFactory) service);
			super.removedService(reference, service);
		}
	}
}
