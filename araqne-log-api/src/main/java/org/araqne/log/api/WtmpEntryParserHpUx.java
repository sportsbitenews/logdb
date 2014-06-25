package org.araqne.log.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;

import org.araqne.log.api.WtmpEntry.Type;

/**
 * HP-UX 11.23 or later
 */
public class WtmpEntryParserHpUx extends WtmpEntryParser {

	@Override
	public int getBlockSize() {
		return 652;
	}

	@Override
	public WtmpEntry parseEntry(ByteBuffer bb) throws IOException {
		bb.getInt();
		byte[] ut_user = new byte[257];
		bb.get(ut_user);
		byte[] padding = new byte[3];
		bb.get(padding);
		int pid = bb.getInt();
		byte[] idBlob = new byte[4];
		bb.get(idBlob);
		byte[] lineBlob = new byte[65];
		bb.get(lineBlob);
		padding = new byte[1];
		bb.get(padding);
		int type = bb.getShort();
		padding = new byte[12];
		bb.get(padding);
		int time = bb.getInt();
		byte[] pad = new byte[24];
		bb.get(pad);
		byte[] hostBlob = new byte[257];
		bb.get(hostBlob);

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
