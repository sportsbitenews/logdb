package org.araqne.storage.localfile;

import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageUtil;
import org.araqne.storage.api.URIResolver;

public class LocalFileURIResolver implements URIResolver{

	@Override
	public FilePath resolveFilePath(String path) {
		String protocol = StorageUtil.extractProtocol(path);
		if (protocol == null || protocol.equals(LocalFilePath.PROTOCOL_STRING))
			return new LocalFilePath(path);
		
		return null;
	}

	@Override
	public String getProtocol() {
		return LocalFilePath.PROTOCOL_STRING;
	}

}
