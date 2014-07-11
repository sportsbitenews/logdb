package org.araqne.log.api;

import java.util.Map;

public interface Reconfigurable {
	void onConfigChange(Map<String, String> oldConfigs, Map<String, String> newConfigs);
}
