package org.araqne.logstorage;

public class TableIDNotFoundException extends TableNotFoundException {
	private static final long serialVersionUID = 1L;
	private int tableID;

	public TableIDNotFoundException(int tableID) {
		super("ID:" + tableID);
		this.tableID = tableID;
	}

	public int getTableID() {
		return tableID;
	}

}
