/**
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
package org.araqne.log.api;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.araqne.log.api.impl.FileUtils;

public class MultiRotationFileLogger extends AbstractLogger implements Reconfigurable {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(MultiRotationFileLogger.class);

	public MultiRotationFileLogger(LoggerSpecification spec, LoggerFactory factory) {
		super(spec, factory);
	}

	@Override
	public void onConfigChange(Map<String, String> oldConfigs, Map<String, String> newConfigs) {
		// purge old states if base path is changed
		if (!oldConfigs.get("base_path").equals(newConfigs.get("base_path"))
				|| !oldConfigs.get("filename_pattern").equals(newConfigs.get("filename_pattern"))) {
			setStates(new HashMap<String, Object>());
		}
	}

	@Override
	protected void runOnce() {
		Map<String, String> configs = getConfigs();

		String basePath = configs.get("base_path");
		String charset = configs.get("charset");
		if (charset == null)
			charset = "utf-8";

		Pattern fileNamePattern = Pattern.compile(configs.get("filename_pattern"));
		Receiver receiver = new Receiver();
		receiver.fileTag = configs.get("file_tag");

		MultilineLogExtractor extractor = MultilineLogExtractor.build(this, receiver);

		List<File> logFiles = FileUtils.matches(basePath, fileNamePattern);
		Map<String, RotationState> rotationStates = RotationStateHelper.deserialize(getStates());

		for (File f : logFiles) {
			receiver.fileName = f.getName();
			processFile(f, charset, extractor, rotationStates);
		}

		setStates(RotationStateHelper.serialize(rotationStates));
	}

	private void processFile(File f, String charset, MultilineLogExtractor extractor, Map<String, RotationState> rotationStates) {
		if (!f.canRead()) {
			slog.debug("araqne log api: multi-rotation logger [{}] file no read permission", getFullName(), f.getAbsolutePath());
			return;
		}

		RotationState oldState = rotationStates.get(f.getName());

		String firstLine = readFirstLine(f, charset);
		long fileLength = f.length();
		long offset = 0;

		if (oldState != null) {
			if (firstLine == null || !firstLine.equals(oldState.getFirstLine()) || fileLength < oldState.getLastLength())
				offset = 0;
			else
				offset = oldState.getLastPosition();
		}

		AtomicLong lastPosition = new AtomicLong(offset);

		FileInputStream is = null;

		try {
			is = new FileInputStream(f);
			is.skip(offset);
			extractor.extract(is, lastPosition);
		} catch (Throwable t) {
			slog.error("araqne log api: multi-rotation logger [" + getFullName() + "] cannot read file", t);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
				}
			}

			RotationState newState = new RotationState(firstLine, lastPosition.get(), fileLength);
			rotationStates.put(f.getName(), newState);
		}
	}

	private String readFirstLine(File f, String charset) {
		FileInputStream is = null;
		BufferedReader br = null;

		try {
			is = new FileInputStream(f);
			br = new BufferedReader(new InputStreamReader(is, charset));
			return br.readLine();
		} catch (Throwable t) {
			slog.error("araqne log api: cannot read first line, multi-rotation logger [" + getFullName() + "]", t);
			return null;
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
				}
			}

			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private class Receiver extends AbstractLogPipe {
		private String fileTag;
		private String fileName;

		@Override
		public void onLog(Logger logger, Log log) {
			if (fileTag != null)
				log.getParams().put(fileTag, fileName);

			write(log);
		}

		@Override
		public void onLogBatch(Logger logger, Log[] logs) {
			if (fileTag != null) {
				for (Log log : logs)
					log.getParams().put(fileTag, fileName);
			}

			writeBatch(logs);
		}
	}
}
