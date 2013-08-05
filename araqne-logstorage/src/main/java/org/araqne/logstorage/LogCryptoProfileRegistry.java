package org.araqne.logstorage;

import java.util.List;

public interface LogCryptoProfileRegistry {
	List<LogCryptoProfile> getProfiles();

	LogCryptoProfile getProfile(String name);

	void addProfile(LogCryptoProfile profile);

	void removeProfile(String name);
}
