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
		int pid = swap(bb.getInt());
		byte[] b = new byte[32];
		bb.get(b);
		byte[] id = new byte[4];
		bb.get(id);
		byte[] user = new byte[32];
		bb.get(user);
		byte[] host = new byte[256];
		bb.get(host);
		bb.getInt(); 
		int session = swap(bb.getInt());
 		int seconds = swap(bb.getInt());
		bb.getInt(); 
		bb.get(new byte[36]); 

		return new WtmpEntry(Type.values()[type], new Date(seconds * 1000L) , pid, readString(user), readString(host), session);
		
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
