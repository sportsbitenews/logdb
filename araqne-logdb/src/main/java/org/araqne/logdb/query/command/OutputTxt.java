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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class OutputTxt extends QueryCommand {
	private final Logger logger = LoggerFactory.getLogger(OutputTxt.class.getName());
	private OutputStream os;
	private String[] fields;
	private BufferedOutputStream bos;
	private OutputStreamWriter osw;
	private SimpleDateFormat sdf;
	private String lineSeparator;
	private String delimiter;
	private String encoding;
	private File f;
	private String filePath;
	private String tmpPath;
	private boolean overwrite;
	private boolean usePartition;
	private boolean useCompression;
	private List<PartitionPlaceholder> holders;
	private Map<List<String>, PartitionOutput> outputs;
	private FileMover mover;

	public OutputTxt(File f, String filePath, String tmpPath, boolean overwrite, String delimiter,
			List<String> fields, boolean useCompression, String encoding, boolean usePartition, List<PartitionPlaceholder> holders)
			throws IOException {
		try {
			this.usePartition = usePartition;
			this.useCompression = useCompression;
			this.encoding = encoding;
			this.f = f;
			this.filePath = filePath;
			this.tmpPath = tmpPath;
			this.overwrite = overwrite;
			this.fields = fields.toArray(new String[0]);
			if (!usePartition) {
				File tartgetFile = f;
				if (tmpPath != null)
					tartgetFile = new File(tmpPath);

				if (useCompression) {
					this.os = new GZIPOutputStream(new FileOutputStream(tartgetFile), 8192);
					this.osw = new OutputStreamWriter(os, Charset.forName(encoding));
				} else {
					this.os = new FileOutputStream(tartgetFile);
					this.bos = new BufferedOutputStream(os);
					this.osw = new OutputStreamWriter(bos, Charset.forName(encoding));
				}
				mover = new LocalFileMover();
			} else {
				this.holders = holders;
				this.outputs = new HashMap<List<String>, PartitionOutput>();
			}

			this.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
			this.lineSeparator = System.getProperty("line.separator");
			this.delimiter = delimiter;
		} catch (Throwable t) {
			close();
			throw new QueryParseException("io-error", -1);
		}
	}

	@Override
	public String getName() {
		return "outputtxt";
	}

	public File getTxtFile() {
		return f;
	}

	public List<String> getFields() {
		return Arrays.asList(fields);
	}

	public String getDelimiter() {
		return delimiter;
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
		Date date = m.getDate();
		OutputStreamWriter writer = osw;
		if (usePartition) {
			List<String> key = new ArrayList<String>(holders.size());
			for (PartitionPlaceholder holder : holders)
				key.add(holder.getKey(date));

			PartitionOutput output = outputs.get(key);
			if (output == null) {
				output = new PartitionOutput(filePath, tmpPath, date, encoding, useCompression);
				outputs.put(key, output);
				logger.debug("araqne logdb: new partition found key [{}] tmpPath [{}] filePath [{}] date [{}]", new Object[] {
						key, tmpPath, filePath, date });
			}

			writer = output.getWriter();
		}

		int n = 0;
		int last = fields.length - 1;
		for (String field : fields) {
			Object o = m.get(field);
			String s = o == null ? "" : o.toString();
			if (o instanceof Date)
				s = sdf.format(o);
			writer.write(s);
			if (n++ != last)
				writer.write(delimiter);
		}
		writer.write(lineSeparator);
	}

	@Override
	public boolean isReducer() {
		return true;
	}

	@Override
	public void onClose(QueryStopReason reason) {
		close();
		if (reason == QueryStopReason.CommandFailure)
			f.delete();
	}

	private void close() {
		if (!usePartition) {
			IoHelper.close(osw);
			IoHelper.close(bos);
			IoHelper.close(os);

			try {
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
		if (useCompression)
			compressionOption = " gz=t";

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
		if (fields.length > 0)
			fieldsOption = " " + Strings.join(getFields(), ", ");

		return "outputtxt" + overwriteOption + compressionOption + delimiterOption + partitionOption + tmpPathOption + path
				+ fieldsOption;
	}
}
