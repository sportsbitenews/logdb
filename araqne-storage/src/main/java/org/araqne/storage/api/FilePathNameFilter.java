package org.araqne.storage.api;

public interface FilePathNameFilter {
	boolean accept(FilePath dir, String name);
}
