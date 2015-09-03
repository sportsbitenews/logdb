package org.araqne.logparser.syslog.symantec;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class ProtectionEngineLogParserTest {
	@Test
	public void testInfectionLog() {
		String line = "1439446303|2|2|3|8|2|4|\\\\?\\FNC\\10.26.100.118\\HOHO$\\vol\\o_vol5\\Q4A0\\Q4A0\\MYMY.exe|5|2|6|ACROBA~1.EXE|7|0|9|Downloader|10|26637|21|20150811.002|37|S-1-5-21-4095124802-1550012173-1094943779-1618|38|WER-PC|39|10.113.80.166|17|33.822|18|33.834|43|10.150.177.55|44|0|45|10202246|64|Malware|66|Virus|65|0|67|Programs that infect other programs, files, or areas of a computer by inserting themselves or attaching themselves to that medium.|68|High|69|High|70|High|71|High|72|High";
		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		ProtectionEngineLogParser p = new ProtectionEngineLogParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("Downloader", m.get("virus_name"));
		assertEquals("ACROBA~1.EXE", m.get("component_name"));
	}
}
