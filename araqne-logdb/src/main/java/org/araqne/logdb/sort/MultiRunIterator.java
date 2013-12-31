/*
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
package org.araqne.logdb.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class MultiRunIterator implements CloseableIterator {
	private List<CloseableIterator> runs = new ArrayList<CloseableIterator>();
	private int runIndex;
	private CloseableIterator current;

	public MultiRunIterator(List<CloseableIterator> runs) {
		this.runs = runs;
		this.current = runs.get(0);
	}

	@Override
	public boolean hasNext() {
		while (true) {
			boolean b = current.hasNext();
			if (b)
				return true;

			if (++runIndex >= runs.size())
				return false;

			current = runs.get(runIndex);
		}
	}

	@Override
	public Item next() {
		if (!hasNext())
			throw new NoSuchElementException();

		return current.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException {
		for (CloseableIterator it : runs) {
			try {
				it.close();
			} catch (Throwable t) {
			}
		}
	}
}
