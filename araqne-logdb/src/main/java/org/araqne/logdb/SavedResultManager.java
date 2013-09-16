package org.araqne.logdb;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.araqne.logstorage.LogCursor;

public interface SavedResultManager {
	List<SavedResult> getResultList(String owner);

	SavedResult getResult(String guid);

	void saveResult(SavedResult result) throws IOException;

	void deleteResult(String guid) throws IOException;

	LogCursor getCursor(String guid) throws IOException;

}
