/*
 * Copyright 2013 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.log.api;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.araqne.log.api.WtmpEntry.Type;

public class WtmpLogger extends AbstractLogger {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(WtmpLogger.class);
	private final File dataDir;
	private String path;

	public WtmpLogger(LoggerSpecification spec, LoggerFactory factory) {
		super(spec, factory);
		dataDir = new File(System.getProperty("araqne.data.dir"), "araqne-log-api");
		dataDir.mkdirs();

		path = spec.getConfig().get("path");

		// try migration at boot
		File oldLastFile = getLastLogFile();
		if (oldLastFile.exists()) {
			Map<String, LastPosition> lastPositions = LastPositionHelper.readLastPositions(oldLastFile);
			setStates(LastPositionHelper.serialize(lastPositions));
			oldLastFile.renameTo(new File(oldLastFile.getAbsolutePath() + ".migrated"));
		}
	}

	@Override
	protected void runOnce() {
		Map<String, LastPosition> lastPositions = LastPositionHelper.deserialize(getStates());
		LastPosition inform = lastPositions.get(path);
		if (inform == null)
			inform = new LastPosition(path);
		long pos = inform.getPosition();

		File wtmpFile = new File(path);
		if (!wtmpFile.exists()) {
			slog.debug("araqne log api: logger [{}] wtmp file [{}] doesn't exist", getFullName(), path);
			return;
		}

		if (!wtmpFile.canRead()) {
			slog.debug("araqne log api: logger [{}] wtmp file [{}] no read permission", getFullName(), path);
			return;
		}

		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(wtmpFile, "r");
			raf.seek(pos);
			byte[] block = new byte[384];

			while (true) {
				int len = raf.read(block);
				if (len < 0)
					return;

				pos += len;

				while (len < 384) {
					int l = raf.read(block, len, 384 - len);
					if (l < 0)
						return;

					pos += l;
					len += l;
				}

				WtmpEntry e = parseEntry(ByteBuffer.wrap(block));

				Map<String, Object> data = new HashMap<String, Object>();
				data.put("type", e.getType().toString());
				data.put("host", e.getHost());
				data.put("pid", e.getPid());
				data.put("session", e.getSession());
				data.put("user", e.getUser());

				write(new SimpleLog(e.getDate(), getFullName(), data));
			}
		} catch (Throwable t) {
			slog.error("araqne log api: logger [" + getFullName() + "] cannot load wtmp file [" + path + "]", t);
		} finally {
			if (raf != null) {
				try {
					raf.close();
				} catch (IOException e) {
				}
			}
			inform.setPosition(pos);
			lastPositions.put(path, inform);
			setStates(LastPositionHelper.serialize(lastPositions));
		}
	}

	protected File getLastLogFile() {
		return new File(dataDir, "wtmp-" + getName() + ".lastlog");
	}

	private WtmpEntry parseEntry(ByteBuffer bb) throws IOException {
		int type = swap(bb.getShort());
		bb.getShort(); // padding
		int pid = swap(bb.getInt());
		byte[] b = new byte[32];
		bb.get(b);
		byte[] id = new byte[4];
		bb.get(id);
		byte[] user = new byte[32];
		bb.get(user);
		byte[] host = new byte[256];
		bb.get(host);
		bb.getInt(); // skip exit_status

		int session = swap(bb.getInt());
		int seconds = swap(bb.getInt());
		swap(bb.getInt()); // microseconds

		Calendar c = Calendar.getInstance();
		c.set(Calendar.YEAR, 1970);
		c.set(Calendar.MONTH, 0);
		c.set(Calendar.DAY_OF_MONTH, 1);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);
		c.set(Calendar.MILLISECOND, 0);
		c.add(Calendar.SECOND, seconds);
		c.add(Calendar.MILLISECOND, TimeZone.getDefault().getRawOffset());

		bb.get(new byte[36]); // addr + unused padding

		return new WtmpEntry(Type.values()[type], c.getTime(), pid, parse(user), parse(host), session);
	}

	private static int swap(int v) {
		int a = v;
		int b = (a >> 24) & 0xFF;
		int c = (a >> 8) & 0xFF00;
		int d = (a << 8) & 0xFF0000;
		int e = (a << 24) & 0xFF000000;
		return (b | c | d | e);
	}

	public static short swap(short value) {
		short a = value;
		short b = (short) ((a >> 8) & 0xFF);
		short c = (short) ((a << 8) & 0xFF00);
		return (short) (b | c);
	}

	private static String parse(byte[] b) {
		int i = 0;
		while (b[i] != 0)
			i++;

		return new String(b, 0, i);
	}
}
