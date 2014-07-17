package org.araqne.log.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;

import org.araqne.log.api.WtmpEntry.Type;

public class WtmpEntryParserLinux extends WtmpEntryParser {

	@Override
	public int getBlockSize() {
		return 384;
	}

	@Override
	public WtmpEntry parseEntry(ByteBuffer bb) throws IOException {
		int type = Short.reverseBytes(bb.getShort());
		bb.getShort();
		int pid = Integer.reverseBytes(bb.getInt());
		byte[] b = new byte[32];
		bb.get(b);
		byte[] id = new byte[4];
		bb.get(id);
		byte[] user = new byte[32];
		bb.get(user);
		byte[] host = new byte[256];
		bb.get(host);
		bb.getInt();
		int session = Integer.reverseBytes(bb.getInt());
		int seconds = Integer.reverseBytes(bb.getInt());
		bb.getInt();
		bb.get(new byte[36]);

		return new WtmpEntry(Type.values()[type], new Date(seconds * 1000L), pid, readString(user), readString(host), session);

	}
}
