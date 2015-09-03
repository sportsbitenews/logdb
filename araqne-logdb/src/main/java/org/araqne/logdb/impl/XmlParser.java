package org.araqne.logdb.impl;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class XmlParser {

	private XmlParser() {
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> parseXml(String xml) throws Throwable {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(new InputSource(new StringReader(xml)));
		doc.getDocumentElement().normalize();

		Node rootNode = doc.getLastChild();
		return (Map<String, Object>) parseNode(rootNode);
	}

	@SuppressWarnings("unchecked")
	public static Object parseNode(Node node) {
		NodeList list = node.getChildNodes();
		NamedNodeMap attrs = node.getAttributes();
		int childCount = list.getLength();
		int attrCount = attrs != null ? attrs.getLength() : 0;

		if (childCount == 0 && attrCount == 0) {
			String text = node.getTextContent().trim();
			if (text.isEmpty())
				return null;
			return text;
		} else if (childCount == 1 && attrCount == 0 && isTextNode(list.item(0))) {
			String text = list.item(0).getTextContent().trim();
			if (text.isEmpty())
				return null;
			return text;
		}

		Map<String, Object> m = new HashMap<String, Object>();

		for (int i = 0; i < attrCount; i++) {
			Node attr = attrs.item(i);
			if (getNodeName(attr).equals("xmlns"))
				continue;

			m.put(getNodeName(attr), attr.getTextContent().trim());
		}

		for (int i = 0; i < childCount; i++) {
			Node child = list.item(i);

			Object content = parseNode(child);
			if (content == null)
				continue;

			Object v = m.get(getNodeName(child));
			List<Object> children = null;
			if (v instanceof List) {
				children = (List<Object>) v;
			} else {
				children = new ArrayList<Object>();
				if (v != null)
					children.add(v);
				m.put(getNodeName(child), children);
			}
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
		if (isTextNode(node))
			return "_text";
		return node.getNodeName();
	}

	private static boolean isTextNode(Node node) {
		short nodeType = node.getNodeType();
		return nodeType == Node.TEXT_NODE || nodeType == Node.CDATA_SECTION_NODE;
	}
}
