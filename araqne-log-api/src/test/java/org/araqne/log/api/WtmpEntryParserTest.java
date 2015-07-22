package org.araqne.log.api;

import static org.junit.Assert.assertEquals;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.junit.Test;

public class WtmpEntryParserTest {

	@Test
	public void testEpoch() {
		int seconds = 1403239164;

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

		assertEquals(c.getTime(), new Date(seconds * 1000L));

	}

	@Test
	public void testSolaris10Parser() {
		WtmpEntry e;
		List<WtmpEntry> l = parse(new WtmpEntryParserSolaris(), "wtmpx_solaris10");

		e = l.get(0);
		assertEquals("UserProcess", e.getType().toString());
		assertEquals("192.168.10.34", e.getHost());
		assertEquals(7801, e.getPid());
		assertEquals(0, e.getSession());
		assertEquals("root", e.getUser());
		assertEquals("pts/2", e.getDeviceName());
		assertEquals("t200", e.getInitTabId());

		e = l.get(1);
		assertEquals("UserProcess", e.getType().toString());
		assertEquals("192.168.10.34", e.getHost());
		assertEquals(7814, e.getPid());
		assertEquals(0, e.getSession());
		assertEquals("root", e.getUser());
		assertEquals("ftp7814", e.getDeviceName());
		assertEquals("ftp", e.getInitTabId());
	}

	@Test
	public void testAix5Parser() {
		WtmpEntry e;
		List<WtmpEntry> l = parse(new WtmpEntryParserAix(), "wtmp_aix53");

		e = l.get(0);
		assertEquals("UserProcess", e.getType().toString());
		assertEquals("192.168.100.247", e.getHost());
		assertEquals(1380588, e.getPid());
		assertEquals(0, e.getSession());
		assertEquals("bjkim", e.getUser());
		assertEquals("pts/0", e.getInitTabId());
		assertEquals("pts/0", e.getDeviceName());

		e = l.get(1);
		assertEquals("DeadProcess", e.getType().toString());
		assertEquals("192.168.100.247", e.getHost());
		assertEquals(294944, e.getPid());
		assertEquals(0, e.getSession());
		assertEquals("root", e.getUser());
		assertEquals("ftp294944", e.getDeviceName());
		assertEquals("ftp294944", e.getInitTabId());
	}

	@Test
	public void testAix6Parser() {
		WtmpEntry e;
		List<WtmpEntry> l = parse(new WtmpEntryParserAix(), "wtmp_aix61");

		e = l.get(0);
		assertEquals("UserProcess", e.getType().toString());
		assertEquals("192.168.100.247", e.getHost());
		assertEquals(1454278, e.getPid());
		assertEquals(0, e.getSession());
		assertEquals("root", e.getUser());
		assertEquals("pts/0", e.getDeviceName());
		assertEquals("pts/0", e.getInitTabId());

		e = l.get(1);
		assertEquals("UserProcess", e.getType().toString());
		assertEquals("192.168.100.247", e.getHost());
		assertEquals(417800, e.getPid());
		assertEquals(0, e.getSession());
		assertEquals("root", e.getUser());
		assertEquals("ftp417800", e.getDeviceName());
		assertEquals("ftp417800", e.getInitTabId());
	}

	@Test
	public void testHpUx1123Parser() {
		WtmpEntry e;
		List<WtmpEntry> l = parse(new WtmpEntryParserHpUx(), "wtmps_hp23");

		e = l.get(0);
		assertEquals("UserProcess", e.getType().toString());
		assertEquals("192.168.1.16", e.getHost());
		assertEquals(2486, e.getPid());
		assertEquals(0, e.getSession());
		assertEquals("root", e.getUser());
		assertEquals("ftpd2486", e.getDeviceName());
		assertEquals("", e.getInitTabId());

		e = l.get(1);
		assertEquals("LoginProcess", e.getType().toString());
		assertEquals("192.168.1.20", e.getHost());
		assertEquals(2854, e.getPid());
		assertEquals(0, e.getSession());
		assertEquals("LOGIN", e.getUser());
		assertEquals("pts/tb", e.getDeviceName());
		assertEquals("tb", e.getInitTabId());
	}

	@Test
	public void testHpUx1131Parser() {
		WtmpEntry e;
		List<WtmpEntry> l = parse(new WtmpEntryParserHpUx(), "wtmps_hp31");

		e = l.get(0);
		assertEquals("DeadProcess", e.getType().toString());
		assertEquals("", e.getHost());
		assertEquals(13430, e.getPid());
		assertEquals(0, e.getSession());
		assertEquals("dunhill", e.getUser());
		assertEquals("pts/tc", e.getDeviceName());
		assertEquals("tc", e.getInitTabId());

		e = l.get(1);
		assertEquals("DeadProcess", e.getType().toString());
		assertEquals("", e.getHost());
		assertEquals(26155, e.getPid());
		assertEquals(0, e.getSession());
		assertEquals("root", e.getUser());
		assertEquals("pts/ta", e.getDeviceName());
		assertEquals("ta", e.getInitTabId());
	}

	private List<WtmpEntry> parse(WtmpEntryParser parse, String path) {

		WtmpEntryParser parser = parse;
		int blockSize = parser.getBlockSize();
		List<WtmpEntry> l = new ArrayList<WtmpEntry>();
		String dir = "src/test/resources/";
		

		File wtmpFile = new File(dir + path);
		RandomAccessFile raf = null;
		
		try {
			if (!wtmpFile.exists()) 
				throw new FileNotFoundException();

			if (!wtmpFile.canRead()) 
				throw new SecurityException();
			
			raf = new RandomAccessFile(dir + path, "r");
			raf.seek(0);
			byte[] block = new byte[blockSize];

			while (true) {
				raf.readFully(block);
				l.add(parser.parseEntry(ByteBuffer.wrap(block)));
			}
		} catch (EOFException e) {
		}catch (Throwable t) {
			t.printStackTrace();
		} finally {
			if (raf != null) {
				try {
					raf.close();
				} catch (IOException e) {
				}
			}
		}
		return l;
	}
}
