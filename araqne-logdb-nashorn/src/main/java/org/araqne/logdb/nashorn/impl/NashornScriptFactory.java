package org.araqne.logdb.nashorn.impl;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.ServiceProperty;
import org.araqne.api.Script;
import org.araqne.api.ScriptFactory;

@Component(name = "nashorn-script-factory")
@Provides
public class NashornScriptFactory implements ScriptFactory {

	@ServiceProperty(name = "alias", value = "nashorn")
	private String alias;

	@Override
	public Script createScript() {
		return new NashornScript();
	}

}
