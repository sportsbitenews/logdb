package org.araqne.rss.logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.araqne.log.api.AbstractLogger;
import org.araqne.log.api.LoggerFactory;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.SimpleLog;
import org.araqne.rss.RssEntry;
import org.araqne.rss.RssFeed;
import org.araqne.rss.RssReader;

public class RssLogger extends AbstractLogger {
	private RssReader reader;

	public RssLogger(LoggerSpecification spec, LoggerFactory factory, RssReader reader) {
		super(spec, factory);
		this.reader = reader;
	}

	@Override
	protected void runOnce() {
		Map<String, String> configs = getConfigs();
		String rssUrl = configs.get("rss");

		boolean stripTag = false;
		if (configs.get("strip") != null)
			stripTag = Boolean.parseBoolean(configs.get("strip"));

		String key = (String) getStates().get("key");
		String lastKey = null;
		try {
			RssFeed feed = reader.read(rssUrl, stripTag);
			Iterator<RssEntry> it = feed.getEntries();
			while (it.hasNext()) {
				RssEntry entry = it.next();
				if (key != null) {
					if ((entry.getGuid() != null && key.equals(entry.getGuid()))
							|| (entry.getLink() != null && key.equals(entry.getLink())))
						break;
				}

				write(new SimpleLog(entry.getCreatedAt(), getFullName(), entry.toMap()));
				if (lastKey == null) {
					if (entry.getGuid() != null)
						lastKey = entry.getGuid();
					else
						lastKey = entry.getLink();
				}
			}
		} finally {
			if (lastKey != null) {
				Map<String, Object> m = new HashMap<String, Object>();
				m.put("key", lastKey);
				setStates(m);
			}
		}
	}
}
