/*
 * Copyright 2015 Eediom Inc. All rights reserved.
 */
package org.araqne.logdb.metadata;

import java.util.List;
import java.util.Arrays;
import java.util.HashMap;
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
import org.araqne.storage.api.RCDirectBufferManager;

@Component(name = "logdb-memory-metadata")
public class MemoryMetadataProvider implements MetadataProvider, FieldOrdering {
	@Requires
	private MetadataService metadataService;

	@Requires
	private RCDirectBufferManager manager;

	private List<String> fields;

	public MemoryMetadataProvider() {
		this.fields = Arrays.asList("type", "free", "total");
	}

	@Override
	public String getType() {
		return "memory";
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
		if (queryString.equals(" pools")) {
			Map<String, Object> ret = null;
			for (String poolName : manager.getPoolNames()) {
				ret = new HashMap<String, Object>();
				ret.put("available", manager.getAvailablePoolSize(poolName));
				ret.put("using", manager.getUsingPoolSize(poolName));
				ret.put("name", poolName);
				ret.put("type", "offheap");
				callback.onPush(new Row(ret));
			}
		} else if (queryString.equals(" objects")) {
			Map<String, Object> ret = null;
			for (String usageName : manager.getUsagesNames()) {
				ret = new HashMap<String, Object>();
				ret.put("using", manager.getUsingObjectSize(usageName));
				ret.put("name", usageName);
				ret.put("type", "offheap");
				callback.onPush(new Row(ret));
			}
		} else {
			Map<String, Object> heap = new HashMap<String, Object>();
			Runtime runtime = Runtime.getRuntime();
			heap.put("type", "heap");
			heap.put("free", runtime.freeMemory());
			heap.put("total", runtime.totalMemory());
			callback.onPush(new Row(heap));

			Map<String, Object> rc = new HashMap<String, Object>();
			rc.put("type", "offheap");
			rc.put("total", manager.getTotalCapacity());
			rc.put("object_count", manager.getObjectCount());
			callback.onPush(new Row(rc));
		}
	}

	@Override
	public List<String> getFieldOrder() {
		return fields;
	}

}
