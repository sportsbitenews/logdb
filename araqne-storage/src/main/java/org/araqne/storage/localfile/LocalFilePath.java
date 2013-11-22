package org.araqne.storage.localfile;

import java.io.File;
import java.io.IOException;

import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageInputStream;

public class LocalFilePath implements FilePath {
	private final File path;
	
	public LocalFilePath(File path) {
		this.path = path;
	}

	@Override
	public StorageInputStream newInputStream() throws IOException {
		return new LocalFileInputStream(path);
	}

}
