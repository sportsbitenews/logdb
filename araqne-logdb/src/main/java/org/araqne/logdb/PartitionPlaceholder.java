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
import java.util.Date;

/**
 * 
 * @author darkluster
 * 
 */
public class PartitionPlaceholder {
	public enum Source {
		NOW, LOGTIME
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
}
