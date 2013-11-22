package org.araqne.storage.api;

import java.io.IOException;

public interface FilePath {
	StorageInputStream newInputStream() throws IOException;
}
