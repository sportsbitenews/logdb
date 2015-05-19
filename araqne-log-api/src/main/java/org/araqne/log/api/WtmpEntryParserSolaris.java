package org.araqne.log.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;

import org.araqne.log.api.WtmpEntry.Type;

public class WtmpEntryParserSolaris extends WtmpEntryParser {

	@Override
	public int getBlockSize() {
		return 372;
	}

	@Override
	public WtmpEntry parseEntry(ByteBuffer bb) throws IOException {

		byte[] ut_user = new byte[32];
		bb.get(ut_user);
		byte[] idBlob = new byte[4];
		bb.get(idBlob);
		byte[] lineBlob = new byte[32];
		bb.get(lineBlob);
		int pid = bb.getInt();
		int type = bb.getShort();
		/* term */
		bb.getShort(); 
		/* exit */
		bb.getShort(); 
		/* skip */
		bb.getShort(); 
		int time = bb.getInt();
		int session = bb.getInt();
		byte[] pad = new byte[24];
		bb.get(pad);
		/* syslen */
		bb.getShort();
		byte[] hostBlob = new byte[257];
		bb.get(hostBlob);

		return new WtmpEntry(getEntryType(type), new Date(time * 1000L), pid, readString(ut_user), readString(hostBlob), session,
		readString(lineBlob), readString(idBlob));
	
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
		case 10:
			return Type.DownTime;
		default:
			return Type.Unknown;
		}
	}
}




