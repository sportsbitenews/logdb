package org.araqne.logstorage;

public class ReplicaStorageConfig {
	@Override
	public String toString() {
		return String.format("ReplicaStorageConfig [%s, local=%s, remote=%s:%s]", mode,
				tableName, remoteNode, remoteName);
	}

	public ReplicaStorageConfig(String name, ReplicationMode tmode, String remoteNode, String remoteName) {
		tableName = name;
		mode = tmode;
		this.remoteNode = remoteNode;
		this.remoteName = remoteName;
	}

	String tableName;
	String remoteNode;
	String remoteName;
	ReplicationMode mode;
	
	public String tableName() {
		return tableName;
	}
	
	public String remoteNode() {
		return remoteNode;
	}
	
	public String remoteName() {
		return remoteName;
	}
	
	public ReplicationMode mode() {
		return mode;
	}
	
	public static ReplicaStorageConfig parseTableSchema(TableSchema table) {
		if (table.getReplicaStorage() == null)
			return null;

		TableConfig modeConf = table.getReplicaStorage().getConfig("replication_mode");
		if (modeConf == null)
			return null;

		ReplicationMode tmode = ReplicationMode.parse(modeConf.getValue());

		TableConfig tableConf = table.getReplicaStorage().getConfig("replication_table");
		if (tableConf == null)
			throw new IllegalArgumentException("replication_table in replica storage config is null");

		String replicationTable = tableConf.getValue();
		if (replicationTable == null)
			throw new IllegalArgumentException("the value of replication_table in replica storage config is null");

		String[] tokens = replicationTable.split(":", 2);
		if (tokens.length != 2)
			throw new IllegalArgumentException("cannot parse the value of replication_table in replica storage config: "
					+ replicationTable);

		String remoteNode = tokens[0];
		String remoteName = tokens[1];

		return new ReplicaStorageConfig(table.getName(), tmode, remoteNode, remoteName);
	}

}
