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
package org.araqne.logdb.query.command;

import java.util.Collection;
import java.util.HashMap;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;

public class Explode extends QueryCommand {
	private final String arrayFieldName;

	public Explode(String arrayFieldName) {
		this.arrayFieldName = arrayFieldName;
	}

	@Override
	public String getName() {
		return "explode";
	}

	@Override
	public void onPush(Row row) {
		Object o = row.get(arrayFieldName);
		if (o instanceof Collection) {
			Collection<?> c = (Collection<?>) o;
			RowBatch batch = new RowBatch();
			batch.size = c.size();
			batch.rows = new Row[batch.size];
			
			int i = 0;
			for (Object e : c) {
				HashMap<String, Object> copyMap = new HashMap<String, Object>(row.map());
				Row copy = new Row(copyMap);
				copy.put(arrayFieldName, e);
				batch.rows[i++] = copy;
			}
			pushPipe(batch);
		} else if (o instanceof Object[]){
			Object[] a = (Object[]) o;
			RowBatch batch = new RowBatch();
			batch.size = a.length; 
			batch.rows = new Row[batch.size];

			int i = 0;
			for (Object e : a){
				HashMap<String, Object> copyMap = new HashMap<String, Object>(row.map());
				Row copy = new Row(copyMap);
				copy.put(arrayFieldName, e);
				batch.rows[i++] = copy;
			}

			pushPipe(batch);
		} else {
			pushPipe(row);
		}
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		int count = 0;

		// count batch size
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];

				Object o = row.get(arrayFieldName);
				if (o == null)
					continue;

				if (o instanceof Collection) {
					Collection<?> c = (Collection<?>) o;
					count += c.size();
				} else if (o instanceof Object[]){
					Object[] a = (Object[] ) o;
					count += a.length;
				} else
					count++;
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];

				Object o = row.get(arrayFieldName);
				if (o == null)
					continue;

				if (o instanceof Collection) {
					Collection<?> c = (Collection<?>) o;
					count += c.size();
				} else if (o instanceof Object[]){
					Object[] a = (Object[] ) o;
					count += a.length;
				} else
					count++;
			}
		}

		Row[] exploded = new Row[count];
		RowBatch explodedBatch = new RowBatch();
		explodedBatch.size = exploded.length;
		explodedBatch.rows = exploded;
		int index = 0;

		// generate rows
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];

				Object o = row.get(arrayFieldName);
				if (o == null)
					continue;

				if (o instanceof Collection) {
					Collection<?> c = (Collection<?>) o;
					for (Object e : c) {
						HashMap<String, Object> copyMap = new HashMap<String, Object>(row.map());
						Row copy = new Row(copyMap);
						copy.put(arrayFieldName, e);
						exploded[index++] = copy;
					}
				} else if (o instanceof Object[]) {
					Object[] a = (Object[]) o;
					for (Object e : a) {
						HashMap<String, Object> copyMap = new HashMap<String, Object>(row.map());
						Row copy = new Row(copyMap);
						copy.put(arrayFieldName, e);
						exploded[index++] = copy;
					}
				} else {
					exploded[index++] = row;
				}
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];

				Object o = row.get(arrayFieldName);
				if (o == null)
					continue;

				if (o instanceof Collection) {
					Collection<?> c = (Collection<?>) o;
					for (Object e : c) {
						HashMap<String, Object> copyMap = new HashMap<String, Object>(row.map());
						Row copy = new Row(copyMap);
						copy.put(arrayFieldName, e);
						exploded[index++] = copy;
					}
				} else if (o instanceof Object []) {
					Object[] a = (Object[]) o;
					for (Object e : a) {
						HashMap<String, Object> copyMap = new HashMap<String, Object>(row.map());
						Row copy = new Row(copyMap);
						copy.put(arrayFieldName, e);
						exploded[index++] = copy;
					}
				} else {
					exploded[index++] = row;
				}
			}
		}

		pushPipe(explodedBatch);
	}

	@Override
	public String toString() {
		return "explode " + arrayFieldName;
	}
}
