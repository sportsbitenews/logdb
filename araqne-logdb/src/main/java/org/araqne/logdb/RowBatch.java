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
package org.araqne.logdb;

/**
 * RowBatch iterate example
 * <pre>
 * {@code
if (rowBatch.selectedInUse) {
    for (int i = 0; i < rowBatch.size; i++) {
        int p = rowBatch.selected[i];
        Row row = rowBatch.rows[p];
        operate(row);
    }
} else {
    for (int i = 0; i < rowBatch.size; i++) {
        Row row = rowBatch.rows[i];
        operate(row);
    }
}
}</pre>
*/
public class RowBatch {
	/** A flag whether selected is used or not. default value is false. check RowBatch example code. */
	public boolean selectedInUse;
	/** An array holding seletected row index. check RowBatch example code. */
	public int[] selected;
	/** valid row count */
	public int size;
	/** An array holding rows */
	public Row[] rows;

	/**
	 * @since 2.4.48
	 */
	public RowBatch rebuild() {
		RowBatch c = new RowBatch();
		c.size = size;
		c.rows = new Row[size];

		if (selectedInUse) {
			for (int i = 0; i < size; i++)
				c.rows[i] = rows[selected[i]].clone();
		} else {
			for (int i = 0; i < size; i++)
				c.rows[i] = rows[i].clone();
		}

		return c;
	}
}
