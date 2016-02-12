/**
 * Copyright 2016 Eediom Inc.
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
package org.araqne.logdb.slack;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.ThreadSafe;
import org.json.JSONConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xeraph@eediom.com
 */
public class SendSlackQueryCommand extends QueryCommand implements ThreadSafe {
	private final Logger slog = LoggerFactory.getLogger(SendSlackQueryCommand.class);
	private final URL url;

	public SendSlackQueryCommand(URL url) {
		this.url = url;
	}

	@Override
	public String getName() {
		return "sendslack";
	}

	@Override
	public void onPush(Row row) {
		String text = row.getString("text");
		String channel = row.getString("channel");
		String username = row.getString("username");
		String iconEmoji = row.getString("icon_emoji");

		// text field is required
		if (text == null) {
			pushPipe(row);
			return;
		}

		HttpsURLConnection conn = null;
		try {
			Map<String, Object> m = new HashMap<String, Object>();
			m.put("text", text);
			if (channel != null)
				m.put("channel", channel);

			if (username != null)
				m.put("username", username);

			if (iconEmoji != null)
				m.put("icon_emoji", iconEmoji);

			String json = JSONConverter.jsonize(m);

			conn = (HttpsURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type", "text/json; charset=utf-8");
			conn.setDoOutput(true);
			conn.getOutputStream().write(json.getBytes("utf-8"));
			conn.getOutputStream().flush();

			byte[] b = new byte[8096];
			conn.getInputStream().read(b);
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne logdb slack: cannot post slack message to " + url, t);
		} finally {
			if (conn != null)
				conn.disconnect();
		}

		pushPipe(row);
	}

	@Override
	public String toString() {
		return "sendslack " + url;
	}
}
