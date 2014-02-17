package org.araqne.logdb.query.command;

public class StorageObjectName {
	protected String namespace;
	protected String table;
	protected boolean optional;

	public StorageObjectName(String namespace, String tableName, boolean optional) {
		this.namespace = namespace;
		this.table = tableName;
		this.optional = optional;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((namespace == null) ? 0 : namespace.hashCode());
		result = prime * result + (optional ? 1231 : 1237);
		result = prime * result + ((table == null) ? 0 : table.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StorageObjectName other = (StorageObjectName) obj;
		if (namespace == null) {
			if (other.namespace != null)
				return false;
		} else if (!namespace.equals(other.namespace))
			return false;
		if (optional != other.optional)
			return false;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		return true;
	}

	public String getNamespace() {
		return namespace;
	}

	public String getTable() {
		return table;
	}
	
	public boolean isOptional() {
		return optional;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (namespace != null) {
			sb.append(namespace + ":");
		}
		sb.append(table);
		if (optional) 
			sb.append("?");
		return sb.toString();
	}
}	
