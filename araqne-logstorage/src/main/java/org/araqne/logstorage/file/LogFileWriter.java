/*
 * Copyright 2011 Future Systems
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
package org.araqne.logstorage.file;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.araqne.logstorage.Log;

public abstract class LogFileWriter {
	/**
	 * @since 2.5.0
	 */
	public abstract boolean isLowDisk();

	/**
	 * @since 2.5.0
	 */
	public abstract void purge();

	public abstract long getLastKey();

	public abstract Date getLastDate();

	public abstract Date getLastFlush();

	public abstract long getCount();

	public abstract void write(Log data) throws IOException;

	public abstract void write(List<Log> data) throws IOException;

	public abstract List<Log> getBuffer();

	public boolean flush() throws IOException {
		return flush(false);
	}

	public abstract boolean flush(boolean sweep) throws IOException;

	public abstract void sync() throws IOException;

	public abstract void close() throws IOException;

	public abstract List<List<Log>> getBuffers();

	public abstract boolean isClosed();
}
