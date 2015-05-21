/*
 * Copyright 2012 Future Systems
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.cron.AbstractTickTimer;
import org.araqne.cron.TickService;
import org.araqne.logdb.FileMover;
import org.araqne.logdb.LocalFileMover;
import org.araqne.logdb.PartitionOutput;
import org.araqne.logdb.PartitionPlaceholder;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.Strings;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.writer.CsvLineWriterFactory;
import org.araqne.logdb.writer.LineWriter;
import org.araqne.logdb.writer.LineWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputCsv extends QueryCommand {
	private final Logger logger = LoggerFactory.getLogger(OutputCsv.class.getName());
	private List<String> fields;

	// for query string generation
	private String pathToken;
	private File f;
	private String tmpPath;

	private boolean overwrite;
	private boolean usePartition;
	private boolean useTab;
	private boolean useBom;
	private boolean emptyfile;
	private String encoding;

	private List<PartitionPlaceholder> holders;
	private boolean append;
	private TimeSpan flushInterval;
	private TickService tickService;

	private Map<List<String>, PartitionOutput> outputs;
	private FileMover mover;
	private FlushTimer flushTimer = new FlushTimer();

	private LineWriter writer;
	private LineWriterFactory writerFactory;

	@Deprecated
	public OutputCsv(String pathToken, File f, String tmpPath, boolean overwrite, List<String> fields, String encoding,
			boolean useBom, boolean useTab, boolean usePartition, boolean emptyfile, List<PartitionPlaceholder> holders,
			boolean append, TimeSpan flushInterval, TickService tickService) {
		try {
			this.pathToken = pathToken;
			this.f = f;
			this.tmpPath = tmpPath;
			this.overwrite = overwrite;
			this.fields = fields;
			this.usePartition = usePartition;
			this.encoding = encoding;
			this.holders = holders;
			this.useTab = useTab;
			this.useBom = useBom;
			this.emptyfile = emptyfile;
			char separator = useTab ? '\t' : ',';
			this.append = append;
			this.flushInterval = flushInterval;
			this.writerFactory = new CsvLineWriterFactory(fields, encoding, separator, useBom, append);

			if (flushInterval != null)
				tickService.addTimer(flushTimer);

			if (!usePartition) {
				String path = pathToken;
				if (tmpPath != null)
					path = tmpPath;

				this.writer = writerFactory.newWriter(path);
				mover = new LocalFileMover();
			} else {
				this.holders = holders;
				this.outputs = new HashMap<List<String>, PartitionOutput>();
			}
		} catch (Throwable t) { 
			close();
			throw new QueryParseException("io-error", -1);
		}
	}
	
	public OutputCsv(String pathToken, String tmpPath, boolean overwrite, List<String> fields, String encoding,
			boolean useBom, boolean useTab, boolean usePartition,boolean emptyfile,  List<PartitionPlaceholder> holders,
			boolean append, TimeSpan flushInterval, TickService tickService) {
			this.pathToken = pathToken;
			this.tmpPath = tmpPath;
			this.overwrite = overwrite;
			this.fields = fields;
			this.usePartition = usePartition;
			this.encoding = encoding;
			this.holders = holders;
			this.useTab = useTab;
			this.useBom = useBom;
			this.emptyfile = emptyfile;
			this.append = append;
			this.flushInterval = flushInterval;
		
			if (flushInterval != null)
				tickService.addTimer(flushTimer);
	}

	@Override
	public String getName() {
		return "outputcsv";
	}

	public File getCsvFile() {
		return f;
	}

	public boolean isOverwrite() {
		return overwrite;
	}

	public List<String> getFields() {
		return fields;
	}

	@Override
	public void onStart(){
		File csvFile = new File(pathToken);
		if (csvFile.exists() && !overwrite  && !append)
			throw new IllegalStateException("csv file exists: " + csvFile.getAbsolutePath());

		if (!usePartition && csvFile.getParentFile() != null)
			csvFile.getParentFile().mkdirs();
		
		this.f = csvFile;
		
		char separator = useTab ? '\t' : ',';
		this.writerFactory = new CsvLineWriterFactory(fields, encoding, separator, useBom, append);
		
		try {
			if (!usePartition) {
				String path = pathToken;
				if (tmpPath != null)
					path = tmpPath;

				this.writer = writerFactory.newWriter(path);
				mover = new LocalFileMover();
			} else {
				//this.holders = holders;
				this.outputs = new HashMap<List<String>, PartitionOutput>();
			}
		}catch(QueryParseException t){
			close();
			throw t;
		} catch (Throwable t) {
			close();
			Map<String, String> params = new HashMap<String, String> ();
			params.put("msg", t.getMessage());
			throw new QueryParseException("30203",  -1, -1, params);
			//throw new QueryParseException("io-error", -1);
		}
		
	}
	
	@Override
	public void onPush(Row m) {
		try {
			writeLog(m);
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: cannot write log to csv file", t);

			getQuery().stop(QueryStopReason.CommandFailure);
		}
		pushPipe(m);
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		try {
			if (rowBatch.selectedInUse) {
				for (int i = 0; i < rowBatch.size; i++) {
					int p = rowBatch.selected[i];
					Row m = rowBatch.rows[p];

					writeLog(m);
				}
			} else {
			    for (int i = 0; i < rowBatch.size; i++) {
			        Row m = rowBatch.rows[i];
					writeLog(m);
				}
			}
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: cannot write log to csv file", t);

			getQuery().stop(QueryStopReason.CommandFailure);
		}

		pushPipe(rowBatch);
	}

	private void writeLog(Row m) throws IOException {
		LineWriter writer = this.writer;
		if (usePartition) {
			List<String> key = new ArrayList<String>(holders.size());
			Date date = m.getDate();
			for (PartitionPlaceholder holder : holders)
				key.add(holder.getKey(date));

			PartitionOutput output = outputs.get(key);
			if (output == null) {
				output = new PartitionOutput(writerFactory, pathToken, tmpPath, date, encoding);
				outputs.put(key, output);

				if (logger.isDebugEnabled())
					logger.debug("araqne logdb: new partition found key [{}] tmpPath [{}] filePath [{}] date [{}]", new Object[] {
							key, tmpPath, pathToken, date });
			}

			writer = output.getWriter();
		}

		writer.write(m);
	}

	@Override
	public boolean isReducer() {
		return true;
	}

	@Override
	public void onClose(QueryStopReason reason) {
		close();
		if (!append && reason == QueryStopReason.CommandFailure) {
			if (tmpPath != null)
				new File(tmpPath).delete();
			else
				f.delete();
		}
	}

	private void close() {
		if (flushInterval != null && tickService != null) {
			tickService.removeTimer(flushTimer);
		}

		if (!usePartition) {
			try {
				writer.close();
				if (tmpPath != null) {
					mover.move(tmpPath, pathToken);
				}
			} catch (Throwable t) {
				logger.error("araqne logdb: file move failed", t);
			}
		} else {
			for (PartitionOutput output : outputs.values())
				output.close();
		}
	}

	@Override
	public String toString() {
		String overwriteOption = " ";
		if (overwrite)
			overwriteOption = " overwrite=t";

		String appendOption = "";
		if (append)
			appendOption = " append=t";

		String partitionOption = "";
		if (usePartition)
			partitionOption = " partition=t";

		String tmpPathOption = "";
		if (tmpPath != null)
			tmpPathOption = " tmp=" + tmpPath;

		String bomOption = "";
		if (useBom)
			bomOption = " bom=t";

		String encodingOption = "";
		if (encoding != null)
			encodingOption = " encoding=" + encoding;

		String tabOption = "";
		if (useTab)
			tabOption = " tab=t";

		return "outputcsv" + overwriteOption + appendOption + partitionOption + tmpPathOption + bomOption + encodingOption
				+ tabOption + " " + pathToken + " " + Strings.join(fields, ", ");
	}

	private class FlushTimer extends AbstractTickTimer {

		@Override
		public int getInterval() {
			return (int) flushInterval.getMillis();
		}

		@Override
		public void onTick() {
			try {
				if (writer != null) {
					writer.flush();
				} else {

				}
			} catch (IOException e) {
			}
		}
	}
}
