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
		bb.getShort(); // padding
		int pid = Integer.reverse(bb.getInt());
		byte[] b = new byte[32];
		bb.get(b);
		byte[] id = new byte[4];
		bb.get(id);
		byte[] user = new byte[32];
		bb.get(user);
		byte[] host = new byte[256];
		bb.get(host);
		bb.getInt(); // skip exit_status

		int session = Integer.reverse(bb.getInt());
		int seconds = Integer.reverse(bb.getInt());
		bb.getInt(); // microseconds
		bb.get(new byte[36]); // addr + unused padding

		if(type == 2)
		System.out.println(readString(user) +"\t"+ type + "\t"  + readString(b));
		
		return new WtmpEntry(Type.values()[type], new Date(seconds * 1000L) , pid, readString(user), readString(host), session);
	}
}
