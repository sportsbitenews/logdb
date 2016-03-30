package org.araqne.logdb.query.expr;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.araqne.logdb.QueryContext;
import org.araqne.logdb.Row;
import org.araqne.logdb.VectorizedRowBatch;
import org.araqne.logdb.impl.XmlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class Xpath extends FunctionExpression {
	private final Logger slog = LoggerFactory.getLogger(Xpath.class);

	private Expression xmlExpr;
	private Expression pathExpr;

	public Xpath(QueryContext ctx, List<Expression> exprs) {
		super("xpath", exprs, 2);
		xmlExpr = exprs.get(0);
		pathExpr = exprs.get(1);
	}

	@Override
	public Object evalOne(VectorizedRowBatch vbatch, int i) {
		Object o1 = vbatch.evalOne(xmlExpr, i);
		Object o2 = vbatch.evalOne(pathExpr, i);
		return xpath(o1, o2);
	}

	@Override
	public Object[] eval(VectorizedRowBatch vbatch) {
		Object[] vec1 = vbatch.eval(xmlExpr);
		Object[] vec2 = vbatch.eval(pathExpr);
		Object[] values = new Object[vbatch.size];
		for (int i = 0; i < values.length; i++)
			values[i] = xpath(vec1[i], vec2[i]);

		return values;
	}

	@Override
	public Object eval(Row row) {
		Object o1 = xmlExpr.eval(row);
		Object o2 = pathExpr.eval(row);
		return xpath(o1, o2);
	}

	private Object xpath(Object o1, Object o2) {
		if (o1 == null || o2 == null)
			return null;

		String xml = o1.toString();
		String path = o2.toString();

		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document doc = builder.parse(new InputSource(new StringReader(xml)));
			doc.getDocumentElement().normalize();

			XPath xpath = XPathFactory.newInstance().newXPath();
			NodeList nodes = (NodeList) xpath.evaluate(path, doc, XPathConstants.NODESET);
			int nodeCount = nodes.getLength();
			if (nodes.getLength() == 1)
				return XmlParser.parseNode(nodes.item(0));

			List<Object> l = new ArrayList<Object>(nodeCount);
			for (int i = 0; i < nodes.getLength(); i++) {
				Node node = nodes.item(i);
				l.add(XmlParser.parseNode(node));
			}

			return l;
		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne logdb: xpath failure", t);
			return null;
		}
	}
}
