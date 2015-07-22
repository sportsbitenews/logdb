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
package org.araqne.log.api;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Map;

/**
 * @since 2.4.6
 * @author xeraph
 * 
 */
public class ExecLogger extends AbstractLogger implements Reconfigurable {
	private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExecLogger.class);
	private String command;

	public ExecLogger(LoggerSpecification spec, LoggerFactory factory) {
		super(spec, factory);
		command = spec.getConfig().get("command");
	}

	@Override
	public void onConfigChange(Map<String, String> oldConfigs, Map<String, String> newConfigs) {
		this.command = newConfigs.get("command");
	}

	@Override
	protected void runOnce() {
		Process p = null;
		InputStream is = null;
		try {
			p = Runtime.getRuntime().exec(command);
			is = p.getInputStream();

			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			byte[] b = new byte[8096];

			while (true) {
				if (getStatus() == LoggerStatus.Stopping || getStatus() == LoggerStatus.Stopped) {
					bos.reset();
					break;
				}

				int len = is.read(b);
				if (len < 0)
					break;

				bos.write(b, 0, len);
			}

			byte[] buf = bos.toByteArray();
			String line = new String(buf).trim();
			if (!line.isEmpty()) {
				write(new LineLog(new Date(), getFullName(), line));
			}
		} catch (Throwable t) {
			logger.error("araqne log api: exec logger [" + getFullName() + "] cannot execute command", t);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
				}
			}

			if (p != null)
				p.destroy();
		}
	}
}
