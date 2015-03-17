package org.araqne.storage.hdfs.script;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.ServiceProperty;
import org.araqne.api.Script;
import org.araqne.api.ScriptFactory;
import org.araqne.storage.hdfs.HDFSStorageManager;

@Component(name = "araqne-hdfs-storage-script-factory")
@Provides
public class HDFSStorageScriptFactory implements ScriptFactory {

	@ServiceProperty(name = "alias", value = "storage")
	private String alias;
	
	@Requires
	private HDFSStorageManager hdfsStorageManager;
	
	@Override
	public Script createScript() {
		return new HDFSStorageScript(hdfsStorageManager);
	}

}
