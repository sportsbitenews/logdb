package org.araqne.log.api;

import static org.junit.Assert.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class LastPositionHelperTest {

	@Test
	public void readV1LastPositionTest() {
		List<String> lines = new ArrayList<String>();
		lines.add("D:\\user\\test\\test.lastpos 123156");
		lines.add("/utm/log/test.lastpos 0");

		Map<String, LastPosition> lastPositions = LastPositionHelper.readLastPosition(lines);
		assertNotNull(lastPositions.get("D:\\user\\test\\test.lastpos"));
		assertNotNull(lastPositions.get("/utm/log/test.lastpos"));

		// window
		LastPosition position = lastPositions.get("D:\\user\\test\\test.lastpos");
		assertEquals("D:\\user\\test\\test.lastpos", position.getPath());
		assertEquals(123156l, position.getPosition());
		assertNotNull(position.getLastSeen());

		// linux
		position = lastPositions.get("/utm/log/test.lastpos");
		assertEquals("/utm/log/test.lastpos", position.getPath());
		assertEquals(0, position.getPosition());
		assertNotNull(position.getLastSeen());

	}

	@Test
	public void readV2LastPositionTest() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH:mm:ss");
		List<String> lines = new ArrayList<String>();
		lines.add("ARAQNE_LAST_POS_VER2");
		lines.add("D:\\user\\test\\test.lastpos 123156 2013112817:44:00");
		lines.add("/utm/log/test.lastpos 0 -");
		lines.add("END_FILE");

		Map<String, LastPosition> lastPositions = LastPositionHelper.readLastPosition(lines);
		assertNotNull(lastPositions.get("D:\\user\\test\\test.lastpos"));
		assertNotNull(lastPositions.get("/utm/log/test.lastpos"));

		// window
		LastPosition position = lastPositions.get("D:\\user\\test\\test.lastpos");
		assertEquals("D:\\user\\test\\test.lastpos", position.getPath());
		assertEquals(123156l, position.getPosition());
		assertEquals("2013112817:44:00", sdf.format(position.getLastSeen()));

		// linux
		position = lastPositions.get("/utm/log/test.lastpos");
		assertEquals("/utm/log/test.lastpos", position.getPath());
		assertEquals(0, position.getPosition());
		assertNotNull(position.getLastSeen());
	}

	@Test
	public void writeLastPositionTest() {
		Date now = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH:mm:ss");
		Map<String, LastPosition> lastPositions = new HashMap<String, LastPosition>();
		LastPosition position1 = new LastPosition("C:\\Windows\\window.lastpos", 123456l, now);
		LastPosition position2 = new LastPosition("/usr/tester/linux.lastpos");
		lastPositions.put(position1.getPath(), position1);
		lastPositions.put(position2.getPath(), position2);

		List<String> lines = LastPositionHelper.parseV2Lines(lastPositions);
		assertEquals(4, lines.size());
		assertEquals("ARAQNE_LAST_POS_VER2", lines.get(0));
		if (lines.get(1).startsWith("C")) {
			assertEquals("C:\\Windows\\window.lastpos 123456 " + sdf.format(now), lines.get(1));
			assertEquals("/usr/tester/linux.lastpos 0 -", lines.get(2));
		}
		else {
			assertEquals("/usr/tester/linux.lastpos 0 -", lines.get(1));
			assertEquals("C:\\Windows\\window.lastpos 123456 " + sdf.format(now), lines.get(2));
		}
		assertEquals("END_FILE", lines.get(3));
	}
}
