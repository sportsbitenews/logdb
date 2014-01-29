package org.araqne.storage.api;

import java.io.IOException;
import java.io.InputStream;

public abstract class StorageInputStream extends InputStream implements StorageInput {
	@Override
	public void readFully(byte[] b) throws IOException {
		readFully(b, 0, b.length);
	}
}
