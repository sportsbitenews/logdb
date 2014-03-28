/**
 * Copyright 2014 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 * @author darkluster
 * 
 */
public class PartitionPlaceholder {
	public enum Source {
		NOW, LOGTIME;

		public static Source parse(String s) {
			if (s == null)
				return null;

			if (s.equals("now"))
				return NOW;
			else if (s.equals("logtime"))
				return LOGTIME;
			return null;
		}
	};

	public enum Format {
		EPOCH, CUSTOM
	}

	private final Source source;
	private final Format format;
	private final String dateFormat;
	private final Date now;

	public PartitionPlaceholder(Source source, String dateFormat) {
		this.source = source;
		this.format = dateFormat.equals("epoch") ? Format.EPOCH : Format.CUSTOM;
		this.dateFormat = dateFormat;
		this.now = new Date();
	}

	/**
	 * return partition key string
	 * 
	 * @param day
	 *            must not null
	 * @return parameterized path
	 */
	public String getKey(Date day) {
		if (format == Format.EPOCH) {
			long l = source == Source.NOW ? now.getTime() : day.getTime();
			return Long.toString(l);
		} else {
			Date target = day;
			if (source == Source.NOW)
				target = now;

			SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
			return sdf.format(target);
		}
	}

	public static List<PartitionPlaceholder> parse(String path) {
		List<PartitionPlaceholder> holders = new ArrayList<PartitionPlaceholder>();
		Pattern p = Pattern.compile("\\{(logtime|now):(.*?)\\}");
		Matcher m = p.matcher(path);

		while (m.find()) {
			String source = m.group(1);
			String format = m.group(2);
			PartitionPlaceholder holder = new PartitionPlaceholder(Source.parse(source), format);
			holders.add(holder);
		}
		return holders;
	}

}
