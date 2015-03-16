package org.araqne.logdb.metadata;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.FieldOrdering;
import org.araqne.logdb.MetadataCallback;
import org.araqne.logdb.MetadataProvider;
import org.araqne.logdb.MetadataService;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.TableSchema;
import org.araqne.logstorage.file.LogBlock;
import org.araqne.logstorage.file.LogBlockCursor;
import org.araqne.logstorage.file.LogFileReader;
import org.araqne.logstorage.file.LogFileServiceV2;
import org.araqne.storage.api.FilePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "logdb-logblock-metadata")
public class LogBlockMetadataProvider implements MetadataProvider, FieldOrdering {
	private final Logger logger = LoggerFactory.getLogger(LogBlockMetadataProvider.class);

	@Requires
	private LogTableRegistry tableRegistry;

	@Requires
	private LogStorage storage;

	@Requires
	private MetadataService metadataService;

	@Requires
	private LogFileServiceRegistry logFileServiceRegistry;

	private List<String> fields;
	
	public LogBlockMetadataProvider() {
		this.fields = Arrays.asList("table", "block_id", "min_time", "max_time", "ver", "reserved");
	}
	
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
		return "logblock";
	}

	@Override
	public void verify(QueryContext context, String queryString) {
	}

	@Override
	public void query(QueryContext context, String queryString, MetadataCallback callback) {
		String[] tokens = queryString.split(" ");
		String tableName = tokens[1];

		TableSchema schema = tableRegistry.getTableSchema(tableName, true);
		String type = schema.getPrimaryStorage().getType();

		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		Date day = df.parse(tokens[2], new ParsePosition(0));

		FilePath dir = storage.getTableDirectory(tableName);

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String dateText = dateFormat.format(day);

		FilePath indexPath = dir.newFilePath(dateText + ".idx");
		FilePath dataPath = dir.newFilePath(dateText + ".dat");
		FilePath keyPath = dir.newFilePath(dateText + ".key");

		LogFileReader reader = null;
		LogBlockCursor cursor = null;
		try {
			reader = logFileServiceRegistry.newReader(tableName, type, new LogFileServiceV2.Option(schema.getPrimaryStorage(),
					schema.getMetadata(), tableName, dir, indexPath, dataPath, keyPath));
			cursor = reader.getBlockCursor();

			while (cursor.hasNext()) {
				LogBlock lb = cursor.next();
				Map<String, Object> data = lb.getData();
				Map<String, Object> m = new HashMap<String, Object>();
				m.put("table", tableName);
				m.put("ver", data.get("ver"));
				m.put("block_id", data.get("block_id"));
				m.put("min_time", data.get("min_time"));
				m.put("max_time", data.get("max_time"));
				m.put("log_count", data.get("log_count"));
				m.put("original_size", data.get("original_size"));
				m.put("compressed_size", data.get("compressed_size"));
				m.put("iv", data.get("iv"));
				m.put("signature", data.get("signature"));
				Object reserved = data.get("reserved");
				m.put("reserved", reserved == null? false: reserved);
				callback.onPush(new Row(m));
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
	
	@Override
	public List<String> getFieldOrder() {
		return fields;
	}
}
