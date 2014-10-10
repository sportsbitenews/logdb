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
package org.araqne.logdb.query.command;

import java.io.File;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 2.0.2-SNAPSHOT
 * @author darkluster
 * 
 */
public class Mv extends QueryCommand implements ThreadSafe {
	private final Logger logger = LoggerFactory.getLogger(Mv.class.getName());
	private String from;
	private String to;

	public Mv(String from, String to) {
		this.from = from;
		this.to = to;
	}

	@Override
	public String getName() {
		return "mv";
	}

	@Override
	public void onClose(QueryStopReason reason) {
		if (reason != QueryStopReason.End) {
			logger.error("araqne logdb: invalid query stop, reason [{}]", reason.toString());
			return;
		}

		File fromFile = new File(from);
		File toFile = new File(to);

		toFile.getParentFile().mkdirs();

		if (!fromFile.exists()) {
			logger.error("araqne logdb: mv - source file [{}] not found", fromFile.getAbsolutePath());
			return;
		}

		if (!fromFile.renameTo(toFile)) {
			// TODO: copy using nio channel
			logger.error("araqne logdb: file move failed, from [{}] to [{}]", fromFile.getAbsolutePath(),
					toFile.getAbsolutePath());
		}
	}

	@Override
	public String toString() {
		return "mv from=" + from + " to=" + to;
	}
}
