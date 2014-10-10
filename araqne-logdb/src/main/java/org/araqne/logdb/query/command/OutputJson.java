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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.araqne.logdb.writer.JsonLineWriterFactory;
import org.araqne.logdb.writer.LineWriter;
import org.araqne.logdb.writer.LineWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class OutputJson extends QueryCommand {
	private final Logger logger = LoggerFactory.getLogger(OutputJson.class.getName());
	private List<String> fields;
	private File f;
	private String filePathToken;
	private boolean overwrite;
	private String encoding;
	private boolean usePartition;
	private String tmpPath;
	private List<PartitionPlaceholder> holders;
	private Map<List<String>, PartitionOutput> outputs;
	private LineWriterFactory writerFactory;
	private LineWriter writer;
	private FileMover mover;

	@Deprecated
	public OutputJson(File f, String filePathToken, boolean overwrite, List<String> fields, String encoding,
			boolean usePartition, String tmpPath, List<PartitionPlaceholder> holders) {
		this.f = f;
		this.overwrite = overwrite;
		this.filePathToken = filePathToken;
		this.fields = fields;
		this.encoding = encoding;
		this.usePartition = usePartition;
		this.tmpPath = tmpPath;
		this.holders = holders;

		this.writerFactory = new JsonLineWriterFactory(fields, encoding);

		try {
			if (!usePartition) {
				String path = filePathToken;
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

	public OutputJson(String filePathToken, boolean overwrite, List<String> fields, String encoding,
			boolean usePartition, String tmpPath, List<PartitionPlaceholder> holders) {
		this.overwrite = overwrite;
		this.filePathToken = filePathToken;
		this.fields = fields;
		this.encoding = encoding;
		this.usePartition = usePartition;
		this.tmpPath = tmpPath;
		this.holders = holders;
	}

	@Override
	public void onStart(){
		File jsonFile = new File(filePathToken);
		if (jsonFile.exists() && !overwrite)
			throw new IllegalStateException("json file exists: " + jsonFile.getAbsolutePath());

		if (!usePartition && jsonFile.getParentFile() != null)
			jsonFile.getParentFile().mkdirs();
		
		this.f = jsonFile;
		
		this.writerFactory = new JsonLineWriterFactory(fields, encoding);
		try {
			if (!usePartition) {
				String path = filePathToken;
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
			throw new QueryParseException("30303",  -1, -1, params);
			//throw new QueryParseException("io-error", -1);
		}
	}
	
	@Override
	public String getName() {
		return "outputjson";
	}

	public File getTxtFile() {
		return f;
	}

	public List<String> getFields() {
		return fields;
	}

	@Override
	public void onPush(Row m) {
		try {
			writeLog(m);
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: cannot write log to json file", t);

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
				for (Row m : rowBatch.rows) {
					writeLog(m);
				}
			}
		} catch (Throwable t) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: cannot write log to json file", t);

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
				output = new PartitionOutput(writerFactory, filePathToken, tmpPath, date, encoding);
				outputs.put(key, output);
				logger.debug("araqne logdb: new partition found key [{}] tmpPath [{}] filePath [{}] date [{}]", new Object[] {
						key, tmpPath, filePathToken, date });
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
		this.status = Status.Finalizing;
		close();
		if (reason == QueryStopReason.CommandFailure)
			f.delete();
	}

	private void close() {
		if (!usePartition) {
			try {
				writer.close();
				if (tmpPath != null) {
					mover.move(tmpPath, filePathToken);
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
			overwriteOption = " overwrite=t ";

		String encodingOption = "";
		if (encoding != null)
			encoding = " encoding=" + encoding;

		String partitionOption = "";
		if (usePartition)
			partitionOption = " partition=t";

		String tmpOption = "";
		if (tmpPath != null)
			tmpOption = " tmp=" + tmpPath;

		String fieldsOption = "";
		if (!fields.isEmpty())
			fieldsOption = " " + Strings.join(fields, ", ");

		return "outputjson" + overwriteOption + encodingOption + partitionOption + tmpOption + filePathToken + fieldsOption;
	}
}

/**
 * backup 140821 kyun
 */
//public OutputJson(File f, String filePathToken, boolean overwrite, List<String> fields, String encoding,
//		boolean usePartition, String tmpPath, List<PartitionPlaceholder> holders) {
//	this.f = f;
//	this.overwrite = overwrite;
//	this.filePathToken = filePathToken;
//	this.fields = fields;
//	this.encoding = encoding;
//	this.usePartition = usePartition;
//	this.tmpPath = tmpPath;
//	this.holders = holders;
//
//	this.writerFactory = new JsonLineWriterFactory(fields, encoding);
//
//	try {
//		if (!usePartition) {
//			String path = filePathToken;
//			if (tmpPath != null)
//				path = tmpPath;
//
//			this.writer = writerFactory.newWriter(path);
//			mover = new LocalFileMover();
//		} else {
//			this.holders = holders;
//			this.outputs = new HashMap<List<String>, PartitionOutput>();
//		}
//	} catch (Throwable t) {
//		close();
//		throw new QueryParseException("io-error", -1);
//	}
//}
//
//@Override
//public String getName() {
//	return "outputjson";
//}
//
//public File getTxtFile() {
//	return f;
//}
//
//public List<String> getFields() {
//	return fields;
//}
//
//@Override
//public void onStart(){
//	
//}
//
//@Override
//public void onPush(Row m) {
//	try {
//		writeLog(m);
//	} catch (Throwable t) {
//		if (logger.isDebugEnabled())
//			logger.debug("araqne logdb: cannot write log to json file", t);
//
//		getQuery().stop(QueryStopReason.CommandFailure);
//	}
//	pushPipe(m);
//}
//
//@Override
//public void onPush(RowBatch rowBatch) {
//	try {
//		if (rowBatch.selectedInUse) {
//			for (int i = 0; i < rowBatch.size; i++) {
//				int p = rowBatch.selected[i];
//				Row m = rowBatch.rows[p];
//
//				writeLog(m);
//			}
//		} else {
//			for (Row m : rowBatch.rows) {
//				writeLog(m);
//			}
//		}
//	} catch (Throwable t) {
//		if (logger.isDebugEnabled())
//			logger.debug("araqne logdb: cannot write log to json file", t);
//
//		getQuery().stop(QueryStopReason.CommandFailure);
//	}
//
//	pushPipe(rowBatch);
//}
//
//private void writeLog(Row m) throws IOException {
//	LineWriter writer = this.writer;
//	if (usePartition) {
//		List<String> key = new ArrayList<String>(holders.size());
//		Date date = m.getDate();
//		for (PartitionPlaceholder holder : holders)
//			key.add(holder.getKey(date));
//
//		PartitionOutput output = outputs.get(key);
//		if (output == null) {
//			output = new PartitionOutput(writerFactory, filePathToken, tmpPath, date, encoding);
//			outputs.put(key, output);
//			logger.debug("araqne logdb: new partition found key [{}] tmpPath [{}] filePath [{}] date [{}]", new Object[] {
//					key, tmpPath, filePathToken, date });
//		}
//
//		writer = output.getWriter();
//	}
//
//	writer.write(m);
//}
//
//@Override
//public boolean isReducer() {
//	return true;
//}
//
//@Override
//public void onClose(QueryStopReason reason) {
//	this.status = Status.Finalizing;
//	close();
//	if (reason == QueryStopReason.CommandFailure)
//		f.delete();
//}
//
//private void close() {
//	if (!usePartition) {
//		try {
//			writer.close();
//			if (tmpPath != null) {
//				mover.move(tmpPath, filePathToken);
//			}
//		} catch (Throwable t) {
//			logger.error("araqne logdb: file move failed", t);
//		}
//	} else {
//		for (PartitionOutput output : outputs.values())
//			output.close();
//	}
//}
//
//@Override
//public String toString() {
//	String overwriteOption = " ";
//	if (overwrite)
//		overwriteOption = " overwrite=true ";
//
//	String encodingOption = "";
//	if (encoding != null)
//		encoding = " encoding=" + encoding;
//
//	String partitionOption = "";
//	if (usePartition)
//		partitionOption = " partition=t";
//
//	String tmpOption = "";
//	if (tmpPath != null)
//		tmpOption = " tmp=" + tmpPath;
//
//	String fieldsOption = "";
//	if (!fields.isEmpty())
//		fieldsOption = " " + Strings.join(fields, ", ");
//
//	return "outputjson" + overwriteOption + encodingOption + partitionOption + tmpOption + filePathToken + fieldsOption;
//}
//}

