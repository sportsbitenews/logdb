package org.araqne.logstorage.exporter.impl;

import java.io.OutputStream;
import java.io.PrintStream;

public class StandardOutputStream extends PrintStream {
	public StandardOutputStream(OutputStream out) {
		super(out);
	}

	@Override
	public void close() {
	}

}
