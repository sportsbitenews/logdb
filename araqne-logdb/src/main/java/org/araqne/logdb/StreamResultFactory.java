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

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArraySet;

public class StreamResultFactory implements QueryResultFactory {
	private CopyOnWriteArraySet<RowPipe> pipes;

	public StreamResultFactory(RowPipe pipe) {
		pipes = new CopyOnWriteArraySet<RowPipe>();
		pipes.add(pipe);
	}

	public StreamResultFactory(CopyOnWriteArraySet<RowPipe> pipes) {
		this.pipes = pipes;
	}

	@Override
	public QueryResult createResult(QueryResultConfig config) throws IOException {
		return new StreamResult(pipes);
	}

	@Override
	public void registerStorage(QueryResultStorage storage) {
	}

	@Override
	public void unregisterStorage(QueryResultStorage storage) {
	}

	@Override
	public void start() {
	}
}