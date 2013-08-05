package org.araqne.logdb.metadata;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.MetadataCallback;
import org.araqne.logdb.MetadataProvider;
import org.araqne.logdb.MetadataService;
import org.araqne.logstorage.Crypto;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.file.LogBlock;
import org.araqne.logstorage.file.LogBlockCursor;
import org.araqne.logstorage.file.LogFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "logdb-hashcheck-metadata")
public class HashCheckMetadataProvider implements MetadataProvider {
	private final Logger logger = LoggerFactory.getLogger(HashCheckMetadataProvider.class);

	@Requires
	private LogTableRegistry tableRegistry;

	@Requires
	private LogStorage storage;

	@Requires
	private MetadataService metadataService;

	@Requires
	private LogFileServiceRegistry logFileServiceRegistry;

	@Validate
	public void start() {
		metadataService.addProvider(this);
	}

	@Invalidate
	public void stop() {
		if (metadataService != null)
			metadataService.removeProvider(this);
	}

	@Override
	public String getType() {
		return "hashcheck";
	}

	@Override
	public void verify(LogQueryContext context, String queryString) {
	}

	@Override
	public void query(LogQueryContext context, String queryString, MetadataCallback callback) {
		String[] tokens = queryString.split(" ");
		String tableName = tokens[1];
		String type = tableRegistry.getTableMetadata(tableName, LogTableRegistry.LogFileTypeKey);

		File dir = storage.getTableDirectory(tableName);

		Map<String, String> tableMetadata = new HashMap<String, String>();
		for (String key : tableRegistry.getTableMetadataKeys(tableName))
			tableMetadata.put(key, tableRegistry.getTableMetadata(tableName, key));

		for (Date day : storage.getLogDates(tableName)) {

			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			String dateText = dateFormat.format(day);

			File indexPath = new File(dir, dateText + ".idx");
			File dataPath = new File(dir, dateText + ".dat");
			File keyPath = new File(dir, dateText + ".key");

			Map<String, Object> options = new HashMap<String, Object>(tableMetadata);
			options.put("tableName", tableName);
			options.put("indexPath", indexPath);
			options.put("dataPath", dataPath);
			options.put("keyPath", keyPath);

			LogFileReader reader = null;
			LogBlockCursor cursor = null;
			try {
				reader = logFileServiceRegistry.newReader(tableName, type, options);
				cursor = reader.getBlockCursor();

				while (cursor.hasNext()) {
					LogBlock lb = cursor.next();
					Map<String, Object> data = lb.getData();
					Map<String, Object> m = new HashMap<String, Object>();

					byte[] d = (byte[]) data.get("data");
					byte[] signature = (byte[]) data.get("signature");
					if (d != null && signature == null)
						continue;

					String digest = (String) data.get("digest");
					byte[] digestKey = (byte[]) data.get("digest_key");
					byte[] hash = null;
					try {
						if (d != null)
							hash = Crypto.digest(d, d.length, digest, digestKey);
					} catch (Exception e) {
					}

					if (hash != null && Arrays.equals(hash, signature))
						continue;

					m.put("table", tableName);
					m.put("day", day);
					m.put("block_id", data.get("block_id"));
					m.put("signature", signature);
					m.put("hash", hash);
					callback.onLog(new LogMap(m));
				}
			} catch (IOException e) {
				logger.error("araqne logdb: cannot read block metadata", e);
			} finally {
				if (cursor != null) {
					try {
						cursor.close();
					} catch (IOException e) {
					}
				}

				if (reader != null) {
					reader.close();
				}
			}
		}
	}
}
