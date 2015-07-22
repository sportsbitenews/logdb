package org.araqne.log.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.araqne.log.api.impl.FileUtils;

public class ConfigWatchLogger extends AbstractLogger implements Reconfigurable {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(ConfigWatchLogger.class);
	protected String basePath;
	protected Pattern fileNamePattern;

	public ConfigWatchLogger(LoggerSpecification spec, LoggerFactory factory) {
		super(spec, factory);

		basePath = getConfigs().get("base_path");

		String fileNameRegex = getConfigs().get("filename_pattern");
		fileNamePattern = Pattern.compile(fileNameRegex);
	}

	@Override
	protected void runOnce() {
		List<File> files = FileUtils.matches(basePath, fileNamePattern);

		Map<String, ConfigState> states = ConfigState.deserializeMap(getStates());
		for (File f : files)
			processFile(states, f);

		setStates(ConfigState.serializeMap(states));
	}

	@Override
	public void onConfigChange(Map<String, String> oldConfigs, Map<String, String> newConfigs) {
		this.fileNamePattern = Pattern.compile(newConfigs.get("filename_pattern"));
		this.basePath = newConfigs.get("base_path");
		if (!oldConfigs.get("base_path").equals(newConfigs.get("base_path"))) {
			setStates(new HashMap<String, Object>());
		}
	}

	protected void processFile(Map<String, ConfigState> states, File f) {
		String path = f.getAbsolutePath();
		String fullName = getFullName();
		try {
			if (!f.isFile()) {
				slog.debug("araqne log api: logger [{}] skips directory path [{}]", fullName, path);
				return;
			}

			if (!f.canRead()) {
				slog.debug("araqne log api: logger [{}] cannot read file [{}], please check permission", fullName, path);
				return;
			}

			String newHash = hash(f);
			if (slog.isDebugEnabled())
				slog.debug("araqne log api: logger [{}] built hash [{}] for file [{}]", new Object[] { fullName, newHash, path });

			ConfigState oldState = states.get(path);
			if (oldState != null && !oldState.getHash().equals(newHash)) {
				Map<String, Object> params = new HashMap<String, Object>();
				params.put("path", path);
				params.put("old_hash", oldState.getHash());
				params.put("new_hash", newHash);

				write(new SimpleLog(new Date(), fullName, params));
			}

			states.put(path, new ConfigState(newHash));

		} catch (Throwable t) {
			slog.error("araqne log api: logger [" + fullName + "] cannot check file [" + path + "] changes", t);
		}
	}

	private static String hash(File f) throws IOException, NoSuchAlgorithmException {
		FileInputStream is = null;
		try {
			is = new FileInputStream(f);
			MessageDigest md = MessageDigest.getInstance("SHA-1");

			byte[] buf = new byte[8096];
			while (true) {
				int len = is.read(buf, 0, buf.length);
				if (len <= 0)
					break;

				md.update(buf, 0, len);
			}

			byte[] sha1hash = md.digest();
			return convertToHex(sha1hash);
		} finally {
			if (is != null)
				is.close();
		}

	}

	private static String convertToHex(byte[] data) {
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < data.length; i++) {
			int halfbyte = (data[i] >>> 4) & 0x0F;
			int two_halfs = 0;
			do {
				if ((0 <= halfbyte) && (halfbyte <= 9))
					buf.append((char) ('0' + halfbyte));
				else
					buf.append((char) ('a' + (halfbyte - 10)));
				halfbyte = data[i] & 0x0F;
			} while (two_halfs++ < 1);
		}
		return buf.toString();
	}
}