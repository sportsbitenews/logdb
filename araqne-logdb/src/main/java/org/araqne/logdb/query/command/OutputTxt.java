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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.FileMover;
import org.araqne.logdb.PartitionOutput;
import org.araqne.logdb.PartitionPlaceholder;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParseInsideException;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.Strings;
import org.araqne.logdb.writer.GzipLineWriterFactory;
import org.araqne.logdb.writer.LineWriter;
import org.araqne.logdb.writer.LineWriterFactory;
import org.araqne.logdb.writer.PlainLineWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class OutputTxt extends QueryCommand {
	private final Logger logger = LoggerFactory.getLogger(OutputTxt.class.getName());
	//private String[] fields;
	private List<String> fields;
	private String delimiter;
	private String encoding;
	private File f;
	private String filePath;
	private String tmpPath;
	private boolean overwrite;
	private boolean usePartition;
	private boolean useGzip;
	private List<PartitionPlaceholder> holders;
	private Map<List<String>, PartitionOutput> outputs;
	private FileMover mover;

	private LineWriter writer;
	private LineWriterFactory writerFactory;

	@Deprecated
	public OutputTxt(File f, String filePath, String tmpPath, boolean overwrite, String delimiter,
			List<String> fields, boolean useGzip, String encoding, boolean usePartition, List<PartitionPlaceholder> holders) {
		try {
			this.usePartition = usePartition;
			this.useGzip = useGzip;
			this.delimiter = delimiter;
			this.encoding = encoding;
			this.f = f;
			this.filePath = filePath;
			this.tmpPath = tmpPath;
			this.overwrite = overwrite;
		//	this.fields = fields.toArray(new String[0]);
			if (useGzip)
				writerFactory = new GzipLineWriterFactory(fields, delimiter, encoding);
			else
				writerFactory = new PlainLineWriterFactory(fields, delimiter, encoding);

			if (!usePartition) {
				String path = filePath;
				if (tmpPath != null)
					path = tmpPath;

				this.writer = writerFactory.newWriter(path);
			} else {
				this.holders = holders;
				this.outputs = new HashMap<List<String>, PartitionOutput>();
			}

		} catch (Throwable t) {
			close();
			throw new QueryParseException("io-error", -1);
		}
	}
	
	public OutputTxt( String filePath, String tmpPath, boolean overwrite, String delimiter,
			List<String> fields, boolean useGzip, String encoding, boolean usePartition, List<PartitionPlaceholder> holders) {
		
			this.usePartition = usePartition;
			this.useGzip = useGzip;
			this.delimiter = delimiter;
			this.encoding = encoding;
			this.filePath = filePath;
			this.tmpPath = tmpPath;
			this.overwrite = overwrite;
			this.fields = fields;
			//this.fields = fields.toArray(new String[0]);
			
	}

	@Override
	public String getName() {
		return "outputtxt";
	}

	public File getTxtFile() {
		return f;
	}

	public List<String> getFields() {
		return fields;// Arrays.asList(fields);
	}

	public String getDelimiter() {
		return delimiter;
	}

	@Override
	public void onStart(){
		File jsonFile = new File(filePath);
		if (jsonFile.exists() && !overwrite)
			throw new IllegalStateException("json file exists: " + jsonFile.getAbsolutePath());

		if (!usePartition && jsonFile.getParentFile() != null)
			jsonFile.getParentFile().mkdirs();
		
		this.f = jsonFile;
		
		try {
		
			if (useGzip)
				writerFactory = new GzipLineWriterFactory(fields, delimiter, encoding);
			else
				writerFactory = new PlainLineWriterFactory(fields, delimiter, encoding);
			
			if (!usePartition) {
				String path = filePath;
				if (tmpPath != null)
					path = tmpPath;

				this.writer = writerFactory.newWriter(path);
			} else {
			//	this.holders = holders;
				this.outputs = new HashMap<List<String>, PartitionOutput>();
			}
		}catch(QueryParseInsideException t){
			close();
			throw t;
		} catch (Throwable t) {
			close();
			Map<String, String> params = new HashMap<String, String> ();
			params.put("msg", t.getMessage());
			throw new QueryParseException("30405",  -1, -1, params);
			//throw new QueryParseException("io-error", -1);
		}
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
				for (Row m : rowBatch.rows) {
					writeLog(m);
				}
			}
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: cannot write log to txt file", t);

			getQuery().stop(QueryStopReason.CommandFailure);
		}

		pushPipe(rowBatch);
	}

	@Override
	public void onPush(Row m) {
		try {
			writeLog(m);
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: cannot write log to txt file", t);

			getQuery().stop(QueryStopReason.CommandFailure);
		}
		pushPipe(m);
	}

	private void writeLog(Row m) throws IOException {
		LineWriter writer = this.writer;
		if (usePartition) {
			Date date = m.getDate();
			List<String> key = new ArrayList<String>(holders.size());
			for (PartitionPlaceholder holder : holders)
				key.add(holder.getKey(date));

			PartitionOutput output = outputs.get(key);
			if (output == null) {
				output = new PartitionOutput(writerFactory, filePath, tmpPath, date, encoding);
				outputs.put(key, output);

				if (logger.isDebugEnabled())
					logger.debug("araqne logdb: new partition found key [{}] tmpPath [{}] filePath [{}] date [{}]", new Object[] {
							key, tmpPath, filePath, date });
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
		if (reason == QueryStopReason.CommandFailure)
			if (tmpPath != null)
				new File(tmpPath).delete();
			else
				f.delete();
	}

	private void close() {
		if (!usePartition) {
			try {
				this.writer.close();
				if (tmpPath != null) {
					mover.move(tmpPath, filePath);
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
		String overwriteOption = "";
		if (overwrite)
			overwriteOption = " overwrite=t";

		String compressionOption = "";
		if (useGzip)
			compressionOption = " gz=t";

		String encodingOption = "";
		if (encoding != null)
			encodingOption = " encoding=" + encoding;

		String delimiterOption = "";
		if (!delimiter.equals(" "))
			delimiterOption = " delimiter=" + delimiter;

		String partitionOption = "";
		if (usePartition)
			partitionOption = " partition=t";

		String tmpPathOption = "";
		if (tmpPath != null)
			tmpPathOption = " tmp=" + tmpPath;

		String path = " " + filePath;

		String fieldsOption = "";
		//if (fields.length > 0)
		if (fields.size() > 0)
			fieldsOption = " " + Strings.join(getFields(), ", ");

		return "outputtxt" + overwriteOption + encodingOption + compressionOption + delimiterOption + partitionOption
				+ tmpPathOption + path + fieldsOption;
	}
}
