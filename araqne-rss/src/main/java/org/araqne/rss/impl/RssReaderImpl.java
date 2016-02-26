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
package org.araqne.rss.impl;

import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.araqne.rss.FeedType;
import org.araqne.rss.RssCategory;
import org.araqne.rss.RssChannel;
import org.araqne.rss.RssEntry;
import org.araqne.rss.RssFeed;
import org.araqne.rss.RssReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

@Component(name = "araqne-rss-reader")
@Provides
public class RssReaderImpl implements RssReader {
	private final Logger slog = LoggerFactory.getLogger(RssReaderImpl.class);

	@Override
	public RssFeed read(String url, boolean stripTag) {
		return readRssFeed(url.toString(), stripTag);
	}

	private RssFeed readRssFeed(String url, boolean stripTag) {
		try {
			RssFeed feed = new RssFeed();

			DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();
			XPathFactory xpathFactory = XPathFactory.newInstance();

			Document rssXml = builder.parse(url);
			feed.setType(getFeedType(rssXml));
			feed.setChannel(getChannel(rssXml, xpathFactory, feed.getType()));

			setEntries(feed, xpathFactory, rssXml, stripTag);

			return feed;
		} catch (Throwable t) {
			slog.error("araqne-rss: cannot read rss feed [" + url.toString() + "]", t);
			throw new IllegalStateException("cannot read rss feed");
		}
	}

	private void setEntries(RssFeed feed, XPathFactory xpathFactory, Document rssXml, boolean stripTag)
			throws XPathExpressionException {
		XPath xpath = xpathFactory.newXPath();
		XPathExpression xPathExpression = xpath.compile(getEntryXPath(feed.getType()));
		NodeList entryNodeList = (NodeList) xPathExpression.evaluate(rssXml, XPathConstants.NODESET);

		for (int i = 0; i < entryNodeList.getLength(); ++i) {
			feed.addEntry(getEntry(feed, entryNodeList.item(i), stripTag));
		}
	}

	private RssEntry getEntry(RssFeed feed, Node entryNode, boolean stripTag) {
		RssEntry entry = null;
		switch (feed.getType()) {
		case ATOM:
			entry = parseAtomEntry(feed, entryNode, stripTag);
			break;
		case RSS2:
			entry = parseRss2Entry(feed, entryNode, stripTag);
			break;
		default:
			entry = parseRss1Entry(feed, entryNode, stripTag);
			break;
		}

		if (entry != null)
			entry.setSource(feed.getChannel().getTitle());

		return entry;
	}

	private RssEntry parseAtomEntry(RssFeed feed, Node entryNode, boolean stripTag) {
		NodeList childNodeList = entryNode.getChildNodes();
		RssEntry entry = new RssEntry();
		for (int i = 0; i < childNodeList.getLength(); ++i) {
			Node childNode = childNodeList.item(i);
			if (childNode.getNodeName() == "author") {
				entry.setAuthor(childNode.getFirstChild().getTextContent());
			} else if (childNode.getNodeName() == "title") {
				entry.setTitle(childNode.getTextContent());
			} else if (childNode.getNodeName() == "link") {
				NamedNodeMap attr = childNode.getAttributes();
				Node linkTypeNode = attr.getNamedItem("rel");
				if (linkTypeNode.getTextContent().equals("alternate")) {
					Node newChildNode = attr.getNamedItem("href");
					entry.setLink(newChildNode.getTextContent());
				}
			} else if (childNode.getNodeName() == "id") {
				entry.setGuid(childNode.getTextContent());
			} else if (childNode.getNodeName() == "published") {
				feed.setIsHaveNotDate(isMatchedDatePattern(childNode.getTextContent()));
				entry.setCreatedAt(RssDateParser.parse(childNode.getTextContent()));
				entry.setIsHaveDateField(true);
			} else if (childNode.getNodeName() == "updated") {
				feed.setIsHaveNotDate(isMatchedDatePattern(childNode.getTextContent()));
				entry.setModifiedAt(RssDateParser.parse(childNode.getTextContent()));
			} else if (childNode.getNodeName() == "content") {
				String textContent = childNode.getTextContent();
				if (stripTag) {
					textContent = textContent.replaceAll("<(/)?([a-zA-Z]*)(\\s[a-zA-Z]*=[^>]*)?(\\s)*(/)?>", "");
					textContent = textContent.replaceAll("<!--(.|\n|\r)*-->", "");
				}
				entry.setContent(textContent.trim());
			}
		}
		return entry;
	}

	private RssEntry parseRss1Entry(RssFeed feed, Node entryNode, boolean stripTag) {
		NodeList childNodeList = entryNode.getChildNodes();
		RssEntry entry = new RssEntry();
		for (int i = 0; i < childNodeList.getLength(); ++i) {
			Node childNode = childNodeList.item(i);
			if (childNode.getNodeName() == "dc:creator") {
				entry.setAuthor(childNode.getTextContent());
			} else if (childNode.getNodeName() == "title") {
				entry.setTitle(childNode.getTextContent());
			} else if (childNode.getNodeName() == "link") {
				entry.setLink(childNode.getTextContent());
			} else if (childNode.getNodeName() == "dc:date") {
				feed.setIsHaveNotDate(isMatchedDatePattern(childNode.getTextContent()));
				entry.setCreatedAt(RssDateParser.parse(childNode.getTextContent()));
				entry.setIsHaveDateField(true);
			} else if (childNode.getNodeName() == "description") {
				String textContent = childNode.getTextContent();
				if (stripTag) {
					textContent = textContent.replaceAll("<(/)?([a-zA-Z]*)(\\s[a-zA-Z]*=[^>]*)?(\\s)*(/)?>", "");
					textContent = textContent.replaceAll("<!--(.|\n|\r)*-->", "");
				}
				entry.setContent(textContent.trim());
			}
		}
		return entry;
	}

	private RssEntry parseRss2Entry(RssFeed feed, Node entryNode, boolean stripTag) {
		NodeList childNodeList = entryNode.getChildNodes();
		RssEntry entry = new RssEntry();
		for (int i = 0; i < childNodeList.getLength(); ++i) {
			Node childNode = childNodeList.item(i);
			if (childNode.getNodeName() == "author") {
				entry.setAuthor(childNode.getTextContent());
			} else if (childNode.getNodeName() == "title") {
				String textContent = childNode.getTextContent();
				String textContent2 = textContent.replaceAll("<!\\[CDATA\\[|\\]\\]>", "");
				String title = textContent2.replaceAll("(<b>|</b>|<font [A-Za-z0-9=]*>|</font>)", "");
				entry.setTitle(title);
			} else if (childNode.getNodeName() == "link") {
				entry.setLink(childNode.getTextContent());
			} else if (childNode.getNodeName() == "guid") {
				entry.setGuid(childNode.getTextContent());
			} else if (childNode.getNodeName() == "pubDate") {
				feed.setIsHaveNotDate(isMatchedDatePattern(childNode.getTextContent()));
				entry.setCreatedAt(RssDateParser.parse(childNode.getTextContent()));
				entry.setIsHaveDateField(true);
			} else if (childNode.getNodeName() == "dc:date") {
				feed.setIsHaveNotDate(isMatchedDatePattern(childNode.getTextContent()));
				entry.setCreatedAt(RssDateParser.parse(childNode.getTextContent()));
				entry.setIsHaveDateField(true);
			} else if (childNode.getNodeName() == "description") {
				String textContent = childNode.getTextContent();
				if (stripTag) {
					textContent = textContent.replaceAll("<(/)?([a-zA-Z]*)(\\s[a-zA-Z]*=[^>]*)?(\\s)*(/)?>", "");
					textContent = textContent.replaceAll("<!--(.|\n|\r)*-->", "");
				}
				entry.setContent(textContent.trim());
			} else if (childNode.getNodeName() == "category") {
				RssCategory category = new RssCategory();
				category.setName(childNode.getTextContent());
				entry.getCategories().add(category);
			}

		}
		return entry;
	}

	private boolean isMatchedDatePattern(String content) {
		String datePattern1 = "\\d{4}-\\d{2}-\\d{2}";
		String datePattern2 = "\\d{4}-\\d{2}-\\d{2} ";

		return Pattern.matches(datePattern1, content) || Pattern.matches(datePattern2, content);
	}

	private String getEntryXPath(FeedType feedType) {
		switch (feedType) {
		case ATOM:
			return "//entry";
		default:
			return "//item";
		}
	}

	private FeedType getFeedType(Document feed) {
		if (feed.getDocumentElement().getTagName() == "feed")
			return FeedType.ATOM;
		else if (feed.getDocumentElement().getTagName() == "rss")
			return FeedType.RSS2;
		else
			return FeedType.RSS1;
	}

	private RssChannel getChannel(Document rssXml, XPathFactory xpathFactory, FeedType feedType) throws XPathExpressionException {
		RssChannel channel = new RssChannel();
		XPath xpath = xpathFactory.newXPath();
		XPathExpression xPathExpression = xpath.compile(getChannelXPath(feedType));
		NodeList titleNodeList = (NodeList) xPathExpression.evaluate(rssXml, XPathConstants.NODESET);

		String channelTitle = null;
		if (titleNodeList.getLength() != 0)
			channelTitle = titleNodeList.item(0).getTextContent();
		channel.setTitle(channelTitle);

		return channel;
	}

	private String getChannelXPath(FeedType feedType) {
		switch (feedType) {
		case ATOM:
			return "//feed/title";
		default:
			return "//channel/title";
		}
	}
}
