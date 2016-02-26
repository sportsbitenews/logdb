package org.araqne.logstorage.engine;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.confdb.Config;
import org.araqne.confdb.ConfigDatabase;
import org.araqne.confdb.ConfigIterator;
import org.araqne.confdb.ConfigService;
import org.araqne.confdb.Predicates;
import org.araqne.logstorage.LogCryptoProfile;
import org.araqne.logstorage.LogCryptoProfileRegistry;
import org.araqne.storage.crypto.LogCryptoException;
import org.araqne.storage.crypto.LogCryptoService;

@Component(name = "logstorage-crypto-profile-registry")
@Provides
public class LogCryptoProfileRegistryImpl implements LogCryptoProfileRegistry {

	@Requires
	private ConfigService conf;
	
	@Requires
	private LogCryptoService cryptoService;

	private ConcurrentHashMap<String, LogCryptoProfile> profiles = new ConcurrentHashMap<String, LogCryptoProfile>();

	@Validate
	public void start() {
		ConfigDatabase db = conf.ensureDatabase("araqne-logstorage");
		ConfigIterator it = db.findAll(LogCryptoProfile.class);
		for (LogCryptoProfile p : it.getDocuments(LogCryptoProfile.class)) {
			profiles.put(p.getName(), p);
		}
	}

	@Invalidate
	public void stop() {
		profiles.clear();
	}

	@Override
	public List<LogCryptoProfile> getProfiles() {
		return new ArrayList<LogCryptoProfile>(profiles.values());
	}

	@Override
	public LogCryptoProfile getProfile(String name) {
		return profiles.get(name);
	}

	@Override
	public void addProfile(LogCryptoProfile profile) {
		// verify if profile arguments are valid
		if (!new File(profile.getFilePath()).exists())
			throw new IllegalArgumentException("key file is not found");
		
		try {
			if (profile.getCipher() != null)
				cryptoService.newBlockCipher(profile.getCipher(), getRandomBytes(32));
		} catch (LogCryptoException e) {
			throw new IllegalArgumentException("invalid cipher algorithm", e);
		}

		try {
			if (profile.getCipher() != null && profile.getDigest() == null)
				throw new IllegalArgumentException("digest algorithm couldn't be omitted");
			if (profile.getDigest() != null)
				cryptoService.newMacBuilder(profile.getDigest(), getRandomBytes(32));
		} catch (LogCryptoException e) {
			throw new IllegalArgumentException("invalid digest algorithm", e);
		}
		
		try {
			cryptoService.newPkiCipher(profile.getPublicKey(), profile.getPrivateKey());
		} catch (LogCryptoException e) {
			throw new IllegalArgumentException("invalid public or private key", e);
		}

		LogCryptoProfile old = profiles.putIfAbsent(profile.getName(), profile);
		if (old != null)
			throw new IllegalStateException("duplicated crypto profile: " + profile.getName());

		ConfigDatabase db = conf.ensureDatabase("araqne-logstorage");
		db.add(profile);
	}

	private byte[] getRandomBytes(int i) {
		byte[] result = new byte[i];
		new Random().nextBytes(result);
		return result;
	}

	@Override
	public void removeProfile(String name) {
		ConfigDatabase db = conf.ensureDatabase("araqne-logstorage");
		Config c = db.findOne(LogCryptoProfile.class, Predicates.field("name", name));
		if (c != null)
			c.remove();

		LogCryptoProfile old = profiles.remove(name);
		if (old == null)
			throw new IllegalStateException("crypto profile not found: " + name);

	}
}
