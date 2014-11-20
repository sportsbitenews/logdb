package org.araqne.logdb.metadata;

import java.util.HashMap;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.LookupHandler;
import org.araqne.logdb.LookupHandlerRegistry;
import org.araqne.logdb.MetadataCallback;
import org.araqne.logdb.MetadataProvider;
import org.araqne.logdb.MetadataService;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.impl.QueryHelper;

/**
 * @since 2.4.46
 */
@Component(name = "logdb-lookup-metadata")
public class LookupMetadataProvider implements MetadataProvider {
	@Requires
	private MetadataService metadataService;

	@Requires
	private LookupHandlerRegistry lookupRegistry;

	@Override
	public String getType() {
		return "lookups";
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
		for (String name : lookupRegistry.getLookupHandlerNames()) {
			Map<String, Object> m = new HashMap<String, Object>();
			m.put("name", name);
			callback.onPush(new Row(m));
		}
	}

}
