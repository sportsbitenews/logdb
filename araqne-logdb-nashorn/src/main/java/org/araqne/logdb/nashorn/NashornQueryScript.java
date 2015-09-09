/**
 * Copyright 2015 Eediom Inc.
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
package org.araqne.logdb.nashorn;

import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.RowPipe;
import org.osgi.framework.BundleContext;

public abstract class NashornQueryScript {
	protected BundleContext bc;
	protected RowPipe pipe;

	public void setBundleContext(BundleContext bc) {
		this.bc = bc;
	}

	public void setRowPipe(RowPipe pipe) {
		this.pipe = pipe;
	}

	public void onRow(NashornQueryScript s, Row row) {
	}

	public void onRowBatch(NashornQueryScript s, RowBatch rowBatch) {
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[rowBatch.selected[i]];
				onRow(s, row);
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];
				onRow(s, row);
			}
		}
	}

	public BundleContext getBundleContext() {
		return bc;
	}

	public void pushPipe(Row row) {
		pipe.onRow(row);
	}

	public void pushPipe(RowBatch rowBatch) {
		pipe.onRowBatch(rowBatch);
	}

	public void onStart() {
	}

	public void onClose(NashornQueryScript s, QueryStopReason reason) {
	}
}
