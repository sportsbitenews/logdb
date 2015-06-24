/**
 * Copyright 2015 Eediom Inc.
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
package org.araqne.logdb.query.command;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/**
 * @author xeraph
 */
public class ParseXml extends QueryCommand implements ThreadSafe {
	private final org.slf4j.Logger slog = LoggerFactory.getLogger(ParseXml.class);

	private final String field;
	private final boolean overlay;

	public ParseXml(String field, boolean overlay) {
		this.field = field;
		this.overlay = overlay;
	}

	@Override
	public String getName() {
		return "parsexml";
	}

	@Override
	public void onPush(Row row) {
		pushPipe(parse(row));
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];
				rowBatch.rows[p] = parse(row);
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];
				rowBatch.rows[i] = parse(row);
			}
		}

		pushPipe(rowBatch);
	}

	private Row parse(Row row) {
		Object target = row.get(field);
		if (target == null)
			return row;

		String xml = target.toString();

		try {
			if (overlay)
				row.map().putAll(parse(xml));
			else
				row = new Row(parse(xml));
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne logdb: parsexml failure - " + xml, t);
		}

		return row;
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> parse(String xml) throws Throwable {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(new InputSource(new StringReader(xml)));
		doc.getDocumentElement().normalize();

		Node rootNode = doc.getLastChild();
		return (Map<String, Object>) traverse(rootNode);
	}

	@SuppressWarnings("unchecked")
	private static Object traverse(Node node) {
		NodeList list = node.getChildNodes();
		NamedNodeMap attrs = node.getAttributes();
		int childCount = list.getLength();
		int attrCount = attrs != null ? attrs.getLength() : 0;

		if (childCount == 0 && attrCount == 0) {
			String text = node.getTextContent();
			if (text.isEmpty())
				return null;
			return text;
		} else if (childCount == 1 && attrCount == 0 && list.item(0).getNodeType() == Node.TEXT_NODE) {
			String text = list.item(0).getTextContent();
			if (text.isEmpty())
				return null;
			return text;
		}

		Map<String, Object> m = new HashMap<String, Object>();

		for (int i = 0; i < attrCount; i++) {
			Node attr = attrs.item(i);
			if (getNodeName(attr).equals("xmlns"))
				continue;

			m.put(getNodeName(attr), attr.getTextContent());
		}

		for (int i = 0; i < childCount; i++) {
			Node child = list.item(i);
			List<Object> children = (List<Object>) m.get(getNodeName(child));
			if (children == null) {
				children = new ArrayList<Object>();
				m.put(getNodeName(child), children);
			}

			Object content = traverse(child);
			if (content != null)
				children.add(content);
		}

		for (String key : m.keySet()) {
			Object val = m.get(key);
			if (!(val instanceof List))
				continue;

			List<Object> l = (List<Object>) val;
			if (l != null && l.isEmpty())
				m.put(key, null);

			// TODO: add formal option
			if (l.size() == 1)
				m.put(key, l.get(0));
		}

		if (m.isEmpty())
			return null;

		return m;
	}
	
	private static String getNodeName(Node node) {
		String t = node.getNodeName();
		if (t.equals("#text"))
			return "_text";
		return t;
	}

	@Override
	public String toString() {
		String fieldOpt = "";
		if (field != null && !field.equals("line"))
			fieldOpt = " field=" + field;

		String overlayOpt = "";
		if (overlay)
			overlayOpt = " overlay=t";

		return "parsexml" + fieldOpt + overlayOpt;
	}
}
