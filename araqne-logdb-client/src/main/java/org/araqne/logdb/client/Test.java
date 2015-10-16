package org.araqne.logdb.client;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Test {
	// en, ko, ja, zh
	public static void main(String[] args) {
		try {
			SimpleDateFormat df = new SimpleDateFormat("a-a-c");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
