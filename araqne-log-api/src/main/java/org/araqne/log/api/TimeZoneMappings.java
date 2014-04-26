package org.araqne.log.api;

public class TimeZoneMappings {
	private TimeZoneMappings() {
	}

	public static String getTimeZone(String alias) {
		if (alias == null)
			return null;

		if (alias.equals("KST"))
			return "Asia/Seoul";

		else if (alias.equals("UTC"))
			return "GMT";

		return null;
	}
}
