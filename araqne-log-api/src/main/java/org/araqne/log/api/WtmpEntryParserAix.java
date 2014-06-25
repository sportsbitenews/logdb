package org.araqne.log.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;

import org.araqne.log.api.WtmpEntry.Type;

public class WtmpEntryParserAix extends WtmpEntryParser {

	@Override
	public int getBlockSize() {
		return 648;
	}

	@Override
	public WtmpEntry parseEntry(ByteBuffer bb) throws IOException {
		/* User login name */
		byte[] ut_user = new byte[256];
		bb.get(ut_user);
		byte[] ut_id = new byte[14];
		bb.get(ut_id);
		byte[] ut_line = new byte[64];
		bb.get(ut_line);
		byte[] temp = new byte[2];
		bb.get(temp);
		int pid = bb.getInt();
		int type = bb.getShort(); 
		byte[] padding = new byte[6];
		bb.get(padding);
		int time = bb.getInt(); 
		/* term */
		bb.getShort();
		/* exit */
		bb.getShort();

		byte[] hostBlob = new byte[256]; 
		bb.get(hostBlob);
		
		//int __dbl_word_pad = bb.getInt(); 
		// int[] reservedA = new int[2];
		// int[] reservedV = new int[6];

		return new WtmpEntry(getEntryType(type), new Date(time * 1000L), pid, readString(ut_user), readString(hostBlob), 0); 
	}

	private Type getEntryType(int d) {
		switch (d) {
		case 0:
			return Type.Empty;
		case 1:
			return Type.RunLevel;
		case 2:
			return Type.BootTime;
		case 3:
			return Type.OldTime;
		case 4:
			return Type.NewTime;
		case 5:
			return Type.InitProcess;
		case 6:
			return Type.LoginProcess;
		case 7:
			return Type.UserProcess;
		case 8:
			return Type.DeadProcess;
		case 9:
			return Type.Accounting;
		default:
			return Type.Unknown;
		}
	}
}
