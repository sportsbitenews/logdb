package org.araqne.rss.command;

import java.util.Iterator;

import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.Row;
import org.araqne.rss.RssEntry;
import org.araqne.rss.RssFeed;
import org.araqne.rss.RssReader;

public class RssCommand extends DriverQueryCommand {
	private RssReader rssReader;
	private String rssUrl;
	private boolean strip;

	public RssCommand(RssReader rssReader, String rssUrl, boolean strip) {
		this.rssReader = rssReader;
		this.rssUrl = rssUrl;
		this.strip = strip;
	}

	@Override
	public void run() {
		status = Status.Running;

		try {
			RssFeed feed = rssReader.read(rssUrl, strip);

			Iterator<RssEntry> it = feed.getEntries();
			while (it.hasNext())
				pushPipe(new Row(it.next().toMap()));
		} catch (Throwable t) {
			throw new RuntimeException(t.getMessage());
		}
	}

	@Override
	public String getName() {
		return "rss";
	}

}
