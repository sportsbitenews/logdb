package org.araqne.logdb.cep.query;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.MetadataCallback;
import org.araqne.logdb.MetadataProvider;
import org.araqne.logdb.MetadataService;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.cep.EventKey;

@Component(name = "cep-topic-meta-provider")
public class CepTopicMetadataProvider implements MetadataProvider {
	@Requires
	private MetadataService metadataService;

	@Requires
	private EventContextService eventContextService;

	@Override
	public String getType() {
		return "ceptopics";
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
	public void verify(QueryContext context, String queryString) {
	}

	@Override
	public void query(QueryContext context, String queryString, MetadataCallback callback) {
		EventContextStorage storage = eventContextService.getStorage("mem");

		HashMap<String, Integer> topicMap = new HashMap<String, Integer>();

		for (EventKey key : storage.getContextKeys()) {
			String topic = key.getTopic();
			Integer count = topicMap.get(topic);
			if (count == null)
				topicMap.put(topic, 1);
			else
				topicMap.put(topic, count + 1);
		}

		for (Entry<String, Integer> pair : topicMap.entrySet()) {
			Row row = new Row();
			row.put("topic", pair.getKey());
			row.put("count", pair.getValue());
			callback.onPush(row);
		}
	}
}
