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
package org.araqne.logdb.impl;

import java.util.Comparator;
import java.util.Iterator;

/**
 * @since 1.6.9
 * @author xeraph
 * 
 */
public class TopSelector<E> {
	private final Comparator<E> cmp;
	private final int limit;
	private final PriorityBlockingDeque<E> q;
	private int count;

	public TopSelector(int limit, Comparator<E> cmp) {
		this.limit = limit;
		this.cmp = cmp;
		this.q = new PriorityBlockingDeque<E>(cmp, limit);
	}

	public void add(E o) {
		if (count++ < limit) {
			q.add(o);
		} else {
			E last = q.peekLast();

			if (cmp.compare(o, last) < 0) {
				q.pollLast();
				q.add(o);
			}
		}
	}

	public Iterator<E> getTopEntries() {
		return q.iterator();
	}
}
