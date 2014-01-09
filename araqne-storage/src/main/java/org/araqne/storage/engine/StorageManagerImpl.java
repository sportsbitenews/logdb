package org.araqne.storage.engine;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageManager;
import org.araqne.storage.localfile.LocalFilePath;

@Component(name = "araqne-storage-manager")
@Provides
public class StorageManagerImpl implements StorageManager {
	public String extractProtocol(String path) {
		int index = path.indexOf("://");
		if (index < 0)
			return null;
		
		return path.substring(0, index);
	}
	
	@Override
	public FilePath resolveFilePath(String path) {
		String protocol = extractProtocol(path);
		if (protocol == null || protocol.equals("file://")) {
			return new LocalFilePath(path);
		}
		
		return null;
	}
	
	@Validate
	@Override
	public void start() {
	}
	
	@Invalidate
	@Override
	public void stop() {
	}

}
