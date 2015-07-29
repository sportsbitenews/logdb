package org.araqne.log.api;

public enum Subtype {
	Defult(""), Regex("regex"), LocalDirectory("local-dir"), LocalFile("local-file");

	private String typeName;

	private Subtype(String name) {
		this.typeName = name;
	}

	@Override
	public String toString() {
		return typeName;
	}
}