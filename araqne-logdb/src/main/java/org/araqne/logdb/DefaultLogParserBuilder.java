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
package org.araqne.logdb;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserBuilder;
import org.araqne.log.api.LogParserFactory;
import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.log.api.LogParserRegistry;
import org.araqne.log.api.LoggerConfigOption;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.TableSchema;

/**
 * @since 2.3.0
 * @author xeraph
 * 
 */
public class DefaultLogParserBuilder implements LogParserBuilder {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(DefaultLogParserBuilder.class);

	String parserNameOverride;
	LogParserRegistry parserRegistry;

	String tableName;
	String tableParserName = null;
	String tableParserFactoryName = null;

	LogParserFactory tableParserFactory = null;
	Map<String, String> parserProperty = null;

	boolean bugAlertSuppressFlag = false;

	public DefaultLogParserBuilder(LogParserRegistry parserRegistry, LogParserFactoryRegistry parserFactoryRegistry,
			LogTableRegistry tableRegistry, String tableName) {
		this(parserRegistry, parserFactoryRegistry, tableRegistry, tableName, null);
	}

	public DefaultLogParserBuilder(LogParserRegistry parserRegistry, LogParserFactoryRegistry parserFactoryRegistry,
			LogTableRegistry tableRegistry, String tableName, String parserNameOverride) {
		this.parserNameOverride = parserNameOverride;
		this.parserRegistry = parserRegistry;

		if (tableName != null) {
			TableSchema schema = tableRegistry.getTableSchema(tableName, true);
			this.tableName = tableName;
			this.tableParserName = schema.getMetadata().get("parser");
			this.tableParserFactoryName = schema.getMetadata().get("logparser");

			if (tableParserFactoryName != null) {
				this.tableParserFactory = parserFactoryRegistry.get(tableParserFactoryName);
				if (tableParserFactory != null) {
					parserProperty = new HashMap<String, String>();
					for (LoggerConfigOption configOption : tableParserFactory.getConfigOptions()) {
						String optionName = configOption.getName();
						String optionValue = schema.getMetadata().get(optionName);
						if (configOption.isRequired() && optionValue == null)
							throw new IllegalArgumentException("require table metadata " + optionName);
						parserProperty.put(optionName, optionValue);
					}
				}
			}
		}
	}

	@Override
	public LogParser build() {
		LogParser parser = null;

		if (parserNameOverride != null && parserRegistry.getProfile(parserNameOverride) != null) {
			try {
				return parserRegistry.newParser(parserNameOverride);
			} catch (IllegalStateException e) {
				if (slog.isDebugEnabled())
					slog.debug("araqne logdb: parser profile not found [{}]", parserNameOverride);
			}
		}

		if (tableParserName != null && parserRegistry.getProfile(tableParserName) != null) {
			try {
				parser = parserRegistry.newParser(tableParserName);
			} catch (IllegalStateException e) {
				if (slog.isDebugEnabled())
					slog.debug("aranqe logdb: parser profile not found [{}]", tableParserName);
			}
		}

		if (parser == null && tableParserFactory != null) {
			parser = tableParserFactory.createParser(parserProperty);
		}
		return parser;
	}

	@Override
	public boolean isBugAlertSuppressed() {
		return bugAlertSuppressFlag;
	}

	@Override
	public void suppressBugAlert() {
		bugAlertSuppressFlag = true;
	}

	public String getTableName() {
		return tableName;
	}
}
