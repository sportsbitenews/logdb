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
package org.araqne.logstorage.backup;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.json.JSONConverter;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 2.2.7
 * @author xeraph
 * 
 */
public class FileBackupMedia implements BackupMedia {
	private final Logger logger = LoggerFactory.getLogger(FileBackupMedia.class);
	private File path;

	public FileBackupMedia(File path) {
		this.path = path;
	}

	@Override
	public Set<String> getTableNames() {
		Set<String> tableNames = new HashSet<String>();
		File baseDir = new File(path, "table");
		File[] files = baseDir.listFiles();
		if (files == null)
			return tableNames;

		for (File f : files) {
			if (f.isDirectory()) {
				// TODO: check table metadata
				File metaFile = new File(f, "table-metadata.json");
				Map<String, Object> tableMetadata;
				try {
					tableMetadata = readTableMetadataJson(metaFile);
					String tableName = (String) tableMetadata.get("_tablename");

					tableNames.add(tableName);
				} catch (IOException e) {
					logger.warn("while reading " + metaFile.getAbsolutePath(), e);
				}
			}
		}

		return tableNames;
	}

	private Map<String, Object> readTableMetadataJson(File metaFile) throws IOException {
		if (!metaFile.isFile())
			throw new IOException("table metadata file does not exist: " + metaFile.getAbsolutePath());

		if (!metaFile.canRead())
			throw new IOException("check permission of table metadata file: " + metaFile.getAbsolutePath());

		String config = readAllText(metaFile);

		try {
			JSONObject json = new JSONObject(config);

			Map<String, Object> m = JSONConverter.parse(json);
			return m;
		} catch (JSONException e) {
			throw new IOException("cannot parse metadata file: " + metaFile.getAbsolutePath());
		}
	}

	private String readAllText(File f) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream(10000);
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(f);
			byte[] b = new byte[8096];
			while (true) {
				int len = fis.read(b);
				if (len < 0)
					break;

				bos.write(b, 0, len);
			}

			return new String(bos.toByteArray(), "utf-8");
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
				}
			}
		}
	}
	
	@Override
	public List<MediaFile> getFiles(String tableName) {
		// TODO: check table metadata

		List<MediaFile> mediaFiles = new ArrayList<MediaFile>();
		File tableDir = new File(path + "/table", tableName);
		logger.info("araqne logstorage: get backup file list for table [{}], dir [{}]", tableName, tableDir.getAbsolutePath());

		if (tableDir.listFiles() == null)
			return mediaFiles;

		Stack<File> dirs = new Stack<File>();
		dirs.push(tableDir);

		while (!dirs.empty()) {
			File dir = dirs.pop();
			for (File f : dir.listFiles()) {
				if (f.isDirectory() && !(f.getName().equals(".") || f.getName().equals(".."))) {
					dirs.push(f);
				} else {
					String name = f.getName();
					if (name.endsWith(".idx") || name.endsWith(".dat") || name.endsWith(".key")) {
						String fpath = f.getAbsolutePath();
						String ppath = tableDir.getAbsolutePath();
						if (fpath.startsWith(ppath)) {
							fpath = fpath.substring(ppath.length() + 1);
							fpath = fpath.replace('\\', '/');
							MediaFile bf = new MediaFile(tableName, fpath, f.length());
							mediaFiles.add(bf);
						}
					}
				}
			}
		}

		return mediaFiles;
	}

	@Override
	public long getFreeSpace() {
		return path.getFreeSpace();
	}

	@Override
	public InputStream getInputStream(String tableName, String fileName) throws IOException {
		File file = new File(path, "table/" + tableName + "/" + fileName);
		return new FileInputStream(file);
	}

	private File getMediaFile(TransferRequest req) {
		if (req.getMediaFile() != null)
			return new File(path, "table/" + req.getTableName() + "/" + req.getMediaFile().getFileName());
		return new File(path, "table/" + req.getTableName() + "/" + req.getMediaFileName());
	}

	@Override
	public void copyFromMedia(TransferRequest req) throws IOException {
		StorageFile dst = req.getStorageFile();
		if (dst.getFile().exists())
			throw new IOException("file already exists: " + dst.getFile().getAbsolutePath());

		File dstTmp = new File(dst.getFile().getAbsolutePath() + ".transfer");

		dstTmp.getParentFile().mkdirs();

		File src = getMediaFile(req);

		if (logger.isDebugEnabled())
			logger.debug("araqne logstorage: copy from [{}] to [{}]", src.getAbsolutePath(), dstTmp.getAbsolutePath());

		FileInputStream is = null;
		FileOutputStream os = null;

		try {
			is = new FileInputStream(src);
			os = new FileOutputStream(dstTmp);
			FileChannel srcChannel = is.getChannel();
			FileChannel dstChannel = os.getChannel();
			srcChannel.transferTo(0, req.getMediaFile().getLength(), dstChannel);

		} finally {
			close(is);
			close(os);
		}

		if (!dstTmp.renameTo(dst.getFile())) {
			dstTmp.delete();
			throw new IOException("rename failed, " + dstTmp.getAbsolutePath());
		}
	}

	@Override
	public void copyToMedia(TransferRequest req) throws IOException {
		File dst = getMediaFile(req);
		if (dst.exists())
			throw new IOException("file already exists: " + dst.getAbsolutePath());

		File dstTmp = new File(dst.getAbsolutePath() + ".transfer");

		dstTmp.getParentFile().mkdirs();

		StorageFile src = req.getStorageFile();

		if (src != null) {
			if (logger.isDebugEnabled())
				logger.debug("araqne logstorage: copy from [{}] to [{}]", src.getFile().getAbsolutePath(), dstTmp.getAbsolutePath());

			FileInputStream is = null;
			FileOutputStream os = null;

			try {
				is = new FileInputStream(src.getFile());
				os = new FileOutputStream(dstTmp);
				FileChannel srcChannel = is.getChannel();
				FileChannel dstChannel = os.getChannel();
				srcChannel.transferTo(0, req.getStorageFile().getLength(), dstChannel);
			} finally {
				close(is);
				close(os);
			}

			if (!dstTmp.renameTo(dst)) {
				dstTmp.delete();
				throw new IOException("rename failed, " + dstTmp.getAbsolutePath());
			}

		} else {
			InputStream is = req.getInputStream();
			if (is == null)
				return;

			FileOutputStream fos = null;
			try {
				fos = new FileOutputStream(dst);
				byte[] b = new byte[8096];
				while (true) {
					int len = is.read(b);
					if (len < 0)
						break;

					fos.write(b, 0, len);
				}
			} finally {
				close(fos);
				close(is);
			}
		}
	}

	private void close(Closeable c) {
		try {
			if (c != null)
				c.close();
		} catch (IOException e) {
		}
	}

	@Override
	public Map<String, String> getTableMetadata(String tableName) throws IOException {
		File baseDir = new File(path, "table");
		File[] files = baseDir.listFiles();

		for (File f : files) {
			if (f.isDirectory()) {
				// TODO: check table metadata
				File metaFile = new File(f, "table-metadata.json");
				Map<String, Object> tableMetadata;
				try {
					tableMetadata = readTableMetadataJson(metaFile);
					String tn = (String) tableMetadata.get("_tablename");
					if (tn.equals(tableName)) {
						Map<String, String> metadata = new HashMap<String, String>();
						@SuppressWarnings("unchecked")
						Map<String, Object> mm = (Map<String, Object>) tableMetadata.get("metadata");
						if (mm != null) {
							for (String key : mm.keySet())
								metadata.put(key, mm.get(key) == null ? null : mm.get(key).toString());

							return metadata;
						} else {
							throw new IllegalStateException("cannot read metadata from " + "table-metadata.json"
									+ ": no 'metadata' child element: " + metaFile);
						}
					}
				} catch (IOException e) {
					logger.warn("while reading " + metaFile.getAbsolutePath(), e);
				}
			}
		}
		return null;
	}
}
