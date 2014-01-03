package org.araqne.logdb.query.parser;

import java.util.List;

import org.araqne.logdb.query.command.StorageObjectName;
import org.araqne.logstorage.LogTableRegistry;

public interface StorageObjectSpec {
	public String getSpec();
	
	public List<StorageObjectName> match(LogTableRegistry logTableRegistry);
	
	public String getNamespace();
	
	public String getTable();
}
