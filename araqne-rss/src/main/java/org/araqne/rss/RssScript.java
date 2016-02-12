/*
 * Copyright 2009 NCHOVY
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
package org.araqne.rss;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.araqne.api.Script;
import org.araqne.api.ScriptArgument;
import org.araqne.api.ScriptContext;
import org.araqne.api.ScriptUsage;

public class RssScript implements Script {
	private ScriptContext context;
	private RssReader rssReader;

	public RssScript(RssReader rssReader) {
		this.rssReader = rssReader;
	}

	@ScriptUsage(description = "read rss feed", arguments = {
			@ScriptArgument(name = "rss url", type = "string", description = "rss url") })
	public void read(String[] args) {
		try {
			RssFeed feed = rssReader.read(args[0], false);
			Iterator<RssEntry> it = feed.getEntries();

			context.println(feed.getChannel().getTitle());
			context.println("====================================");
			while (it.hasNext()) {
				RssEntry entry = it.next();
				context.println(fromDate(entry.getCreatedAt()) + " " + entry.getTitle().replaceAll("%", "%%%%"));
			}
		} catch (Exception e) {
			context.println(e.toString());
			e.printStackTrace();
		}
	}

	private String fromDate(Date date) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]");
		return dateFormat.format(date);
	}

	@Override
	public void setScriptContext(ScriptContext context) {
		this.context = context;
	}
}
