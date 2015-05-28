package org.araqne.logdb.query.parser;

import java.util.List;

import org.araqne.logdb.query.command.StorageObjectName;
import org.araqne.logstorage.LogTableRegistry;

public interface StorageObjectSpec extends Cloneable {
	public String getSpec();
	
	public List<StorageObjectName> match(LogTableRegistry logTableRegistry);
	
	public String getNamespace();
	
	public void setNamespace(String ns);
	
	public String getTable();
	
	public Object clone();
	
	public boolean isOptional();
	
	public void setOptional(boolean optional);
}
