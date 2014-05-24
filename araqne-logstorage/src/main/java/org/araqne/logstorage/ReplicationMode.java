package org.araqne.logstorage;

public enum ReplicationMode {
	ACTIVE, STANDBY, STANDALONE, DUMMY;

	public static ReplicationMode parse(String s) {
		if (s == null)
			return null;

		if (s.equalsIgnoreCase("active"))
			return ACTIVE;
		else if (s.equalsIgnoreCase("standby"))
			return STANDBY;
		else if (s.equalsIgnoreCase("standalone"))
			return STANDALONE;
		else
			throw new IllegalArgumentException("cannot parse: " + s);
	}

	public ReplicationMode invert() {
		return this == STANDALONE ? STANDALONE : (this == ACTIVE ? STANDBY : ACTIVE);
	}

	public String toString() {
		return this == STANDALONE ? "standalone" : (this == ACTIVE ? "active" : "standby");
	}
}
