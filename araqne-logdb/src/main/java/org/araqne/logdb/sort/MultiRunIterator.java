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
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

public class MultiRunIterator implements CloseableIterator {
	private List<RunInput> runs = new ArrayList<RunInput>();
	private int runIndex;
	private RunInput current;
	private Item prefetch;

	public MultiRunIterator(List<RunInput> runs) {
		this.runs = runs;
		this.current = runs.get(0);
	}

	@Override
	public boolean hasNext() {
		if (prefetch != null)
			return true;

		while (true) {
			boolean b = current.hasNext();
			if (b) {
				try {
					prefetch = current.next();
				} catch (IOException e) {
					return false;
				}
				return true;
			}

			if (++runIndex >= runs.size())
				return false;

			current = runs.get(runIndex);
		}
	}

	@Override
	public Item next() {
		if (!hasNext())
			throw new NoSuchElementException();

		Item next = prefetch;
		prefetch = null;
		return next;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException {
		for (RunInput it : runs) {
			try {
				it.purge();
			} catch (Throwable t) {
			}
		}
	}

	@Override
	public void reset() {
		for (RunInput run : runs) {
			try {
				run.reset();
			} catch (IOException e) {
				throw new IllegalStateException("MultiRunIterator reset fail");
			}
		}

		this.current = runs.get(0);
		this.runIndex = 0;
		this.prefetch = null;
	}

	private boolean isItemInRun(Item item, Comparator<Item> comparator, RunInput runInput) throws IOException {
		runInput.getFirstItem();
		Item lastItem = runInput.getLastItem();
		
		int compareReulst2 = comparator.compare(item, lastItem);

		boolean result = compareReulst2 <= 0;
		return result;
	}
	
	public MultiRunIterator jump(Item item, Comparator<Item> comparator) throws IOException {
		if(isItemInRun(item, comparator, current)) {
			//do not jump to another run
			//stay at current run
			
			this.current.search(item, comparator);
			//this.current.reset();
			return this;
		} else {
			this.runIndex = 0;
			for(int i = 0; i < runs.size(); i++) {
				RunInput runInput = this.runs.get(i);
				if(isItemInRun(item, comparator, runInput)){
					this.runIndex = i;
					this.current = runInput;
					this.current.search(item, comparator);
					//this.current.reset();
					return this;
				}
			}
		}
		
		this.current = this.runs.get(this.runs.size() -1);
		this.current.search(item, comparator);
		//this.current.reset();
		return this;
	}
}
