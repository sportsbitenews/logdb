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
public class FileStorageBackupMedia implements StorageBackupMedia, Cloneable {
	private final Logger logger = LoggerFactory.getLogger(FileStorageBackupMedia.class);
	private File path;
	private boolean isWorm;
	private Map<String, TableSchema> cachedSchemas;

	public FileStorageBackupMedia(File path) {
		this.path = path;
		this.cachedSchemas = new HashMap<String, TableSchema>();

		File baseDir = new File(path, "table");
		File[] files = baseDir.listFiles();

		if (files == null)
			return;

		for (File tableDir : files) {
			File metaFile = new File(tableDir, "table-metadata.json");
			if (!metaFile.exists() || !metaFile.canRead())
				continue;

			try {
				Map<String, Object> metadata = readTableSchema(metaFile);
				String tableName = (String) metadata.get("table_name");
				TableSchema schema = new TableSchema(metadata, tableDir);
				cachedSchemas.put(tableName, schema);
			} catch (IOException e) {
				logger.error("araqne logstorage: cannot load table metadata", e);
			}
		}
	}

	public FileStorageBackupMedia(File path, boolean isWorm) {
		this(path);
		this.isWorm = isWorm;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		FileStorageBackupMedia obj = (FileStorageBackupMedia) super.clone();
		obj.cachedSchemas = new HashMap<String, TableSchema>();
		Set<String> keys = cachedSchemas.keySet();
		for (String key : keys) {
			if(cachedSchemas.get(key) != null)
				obj.cachedSchemas.put(key, (TableSchema) cachedSchemas.get(key).clone());
		}
		return obj;
	}

	@Override
	public Set<String> getTableNames() {
		return cachedSchemas.keySet();
	}

	private Map<String, Object> readTableSchema(File metaFile) throws IOException {
		if (!metaFile.isFile())
			throw new IOException("table metadata file does not exist: " + metaFile.getAbsolutePath());

		if (!metaFile.canRead())
			throw new IOException("check permission of table metadata file: " + metaFile.getAbsolutePath());

		String config = readAllText(metaFile);

		try {
			JSONObject json = new JSONObject(config);
			return JSONConverter.parse(json);
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
	public boolean exists(String tableName, String fileName) throws IOException {
		try {
			List<StorageMediaFile> mediaFiles = getFiles(tableName);
			if (mediaFiles.size() <= 0)
				return false;

			for (StorageMediaFile mediaFile : mediaFiles) {
				if (fileName.equals(mediaFile.getFileName()))
					return true;
			}
			return false;
		} catch (IOException e) {
			return false;
		}
	}

	@Override
	public List<StorageMediaFile> getFiles(String tableName) throws IOException {
		TableSchema schema = cachedSchemas.get(tableName);
		if (!cachedSchemas.containsKey(tableName))
			throw new IOException("table [" + tableName + "] not found in backup media");

		List<StorageMediaFile> mediaFiles = new ArrayList<StorageMediaFile>();
		File tableDir = schema.dir;
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
							StorageMediaFile bf = new StorageMediaFile(tableName, fpath, f.length());
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
		TableSchema schema = cachedSchemas.get(tableName);
		if (schema == null)
			throw new IOException("table [" + tableName + "] not found in backup media");

		File file = new File(schema.dir, fileName);
		return new FileInputStream(file);
	}

	private File getMediaFile(StorageTransferRequest req) throws IOException {
		String tableName = req.getMediaFile().getTableName();
		TableSchema schema = cachedSchemas.get(tableName);
		if (schema == null)
			throw new IOException("table [" + tableName + "] not found in backup media");

		return new File(schema.dir, req.getMediaFile().getFileName());
	}

	@Override
	public void copyFromMedia(StorageTransferRequest req) throws IOException {
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
			ensureTransferTo(req, srcChannel, dstChannel, req.getMediaFile().getLength());

		} finally {
			close(is);
			close(os);
		}

		if (!dstTmp.renameTo(dst.getFile())) {
			dstTmp.delete();
			throw new IOException("rename failed, " + dstTmp.getAbsolutePath());
		}
	}

	private void ensureTransferTo(StorageTransferRequest req, FileChannel srcChannel, FileChannel dstChannel, long length)
			throws IOException {
		ensureTransferTo(req, srcChannel, dstChannel, length, 0);
	}

	private void ensureTransferTo(StorageTransferRequest req, FileChannel srcChannel, FileChannel dstChannel, long length,
			long copied) throws IOException {
		while (copied < length) {
			if (req.isCancel())
				break;

			copied += srcChannel.transferTo(copied, length - copied, dstChannel);
		}
	}

	@Override
	public void copyToMedia(StorageTransferRequest req) throws IOException {
		StorageFile src = req.getStorageFile();

		if (src != null) {
			File dst = new File(path, "table/" + src.getTableId() + "/" + src.getFileName());
			if (req.isOverwrite()) {
				boolean isDelete = dst.delete();
				if (!isDelete)
					throw new IOException("delete failed, " + dst.getAbsolutePath());
			}
			if (!isWorm)
				copyToDisk(req, src, dst);
			else
				copyToWorm(req, src, dst);
		} else {
			StorageTransferStream stream = req.getToMediaStream();
			File dst = new File(path, "table/" + stream.getTableId() + "/" + stream.getMediaFileName());
			if (req.isOverwrite())
				dst.delete();

			dst.getParentFile().mkdirs();

			InputStream is = stream.getInputStream();
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
	public void deleteFile(String tableName, String fileName) throws IOException {
		TableSchema schema = cachedSchemas.get(tableName);
		if (schema == null)
			throw new IOException("table [" + tableName + "] not found in backup media");

		File dst = new File(schema.dir, fileName);
		boolean isDelete = dst.delete();
		if (!isDelete)
			throw new IOException("delete failed [file:" + dst.getAbsolutePath() + "]");
	}

	@Override
	public boolean isWormMedia() {
		return isWorm;
	}

	@Override
	public boolean isEject() {
		return !path.exists();
	}

	private void copyToDisk(StorageTransferRequest req, StorageFile src, File dst) throws IOException {
		File dstTmp = new File(dst.getAbsolutePath() + ".transfer");
		if (req.isIncremental() && dst.exists()) {
			if (dst.length() == src.getLength())
				return;
			else
				dst.renameTo(dstTmp);
		}

		dstTmp.getParentFile().mkdirs();

		if (logger.isDebugEnabled())
			logger.debug("araqne logstorage: copy from [{}] to [{}]", src.getFile().getAbsolutePath(), dstTmp.getAbsolutePath());

		FileInputStream is = null;
		FileOutputStream os = null;

		try {
			is = new FileInputStream(src.getFile());
			os = new FileOutputStream(dstTmp);
			FileChannel srcChannel = is.getChannel();
			FileChannel dstChannel = os.getChannel();

			if (req.isIncremental())
				ensureTransferTo(req, srcChannel, dstChannel, src.getLength(), dst.length());
			else
				ensureTransferTo(req, srcChannel, dstChannel, src.getLength());
		} finally {
			close(is);
			close(os);
		}

		if (!dstTmp.renameTo(dst)) {
			dstTmp.delete();
			throw new IOException("rename failed, " + dstTmp.getAbsolutePath());
		}
	}

	private void copyToWorm(StorageTransferRequest req, StorageFile src, File dst) throws IOException {
		if (dst.exists())
			throw new IOException("file [" + dst.getAbsolutePath() + "] is already exists");

		dst.getParentFile().mkdirs();

		if (logger.isDebugEnabled())
			logger.debug("araqne logstorage: copy from [{}] to [{}]", src.getFile().getAbsolutePath(), dst.getAbsolutePath());

		FileInputStream is = null;
		FileOutputStream os = null;

		try {
			is = new FileInputStream(src.getFile());
			os = new FileOutputStream(dst);
			FileChannel srcChannel = is.getChannel();
			FileChannel dstChannel = os.getChannel();

			ensureTransferTo(req, srcChannel, dstChannel, src.getLength());
		} finally {
			close(is);
			close(os);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, String> getTableMetadata(String tableName) throws IOException {
		TableSchema schema = cachedSchemas.get(tableName);
		if (schema == null)
			throw new IOException("table [" + tableName + "] not found in backup media");

		return (Map<String, String>) schema.schema.get("metadata");
	}

	private static class TableSchema implements Cloneable {
		private Map<String, Object> schema;
		private File dir;

		public TableSchema(Map<String, Object> schema, File dir) {
			this.schema = schema;
			this.dir = dir;
		}

		@Override
		public Object clone() throws CloneNotSupportedException {
			TableSchema obj = (TableSchema) super.clone();
			obj.schema = new HashMap<String, Object>();
			for (String key : schema.keySet()) {
				obj.schema.put(key, schema.get(key));
			}
			return obj;
		}
	}
}
