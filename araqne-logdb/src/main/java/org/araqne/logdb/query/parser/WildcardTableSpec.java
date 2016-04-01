package org.araqne.logdb.query.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.log.api.WildcardMatcher;
import org.araqne.logdb.query.command.StorageObjectName;
import org.araqne.logstorage.LogTableRegistry;

public class WildcardTableSpec implements TableSpec {
	String namespace;
	String table;
	boolean optional;
	Pattern pattern;
	private String spec;

	public static void main(String[] args) {
		String specStr = "*";
		Matcher matcher = qualifierPattern.matcher(specStr);
		if (matcher.matches()) {
			System.out.println(matcher.group(1));
			System.out.println(matcher.group(2));
			System.out.println(matcher.group(3));
		} else {
			System.out.println("ERROR");
		}
	}

	public Object clone() {
		return new WildcardTableSpec(toString());
	}

	public WildcardTableSpec(String spec) {
		this.spec = spec;

		// XXX
		Matcher m = Pattern.compile("^(?:(`[^`]+`|[\\w\\*]+)\\:|)(`[^`]+`|[^\\.\\?]+)(?:(\\?)|)").matcher(spec);
		if (m.matches()) {
			String n = m.group(1);
			String t = m.group(2);
			boolean o = m.group(3) != null;
			if (n != null && !n.startsWith("`") && !Pattern.matches("^[\\w*]+$", n)) {
				n = "`" + n + "`";
			}
			if (!t.startsWith("`") && !Pattern.matches("^[\\w*]+$", t)) {
				t = "`" + t + "`";
			}
			StringBuffer sb = new StringBuffer();
			if (n != null)
				sb.append(n + ":");
			sb.append(t);
			if (o)
				sb.append("?");
			spec = sb.toString();
		}

		Matcher matcher = qualifierPattern.matcher(spec);
		if (matcher.matches()) {
			namespace = selectNonNull(matcher.group(1), matcher.group(2));
			table = selectNonNull(matcher.group(3), matcher.group(4));
			optional = matcher.group(5) != null;
			pattern = WildcardMatcher.buildPattern(table, true);
		} else {
			throw new IllegalArgumentException();
		}
	}

	private String selectNonNull(String s1, String s2) {
		if (s1 == null)
			return s2;
		else
			return s1;
	}

	public static Pattern qualifierPattern = Pattern
			.compile("^(?:`([^`]+)`|([\\w\\*]+)\\:|)(?:`([^`]+)`|([\\w\\*]+))(?:(\\?)|)");
	public static Pattern unquotedNameConstraint = Pattern.compile("^[\\w\\*]+$");

	private String quote(String tableName) {
		if (tableName == null)
			return null;
		if (unquotedNameConstraint.matcher(tableName).matches()) {
			return tableName;
		} else {
			return "`" + tableName + "`";
		}
	}

	public boolean hasWildcard() {
		return pattern != null;
	}

	public boolean isOptional() {
		return optional;
	}

	public boolean matches(String tableName) {
		if (pattern == null)
			return table.equals(tableName);
		else
			return pattern.matcher(tableName).matches();
	}

	private StorageObjectName resolveWith(String tableName) {
		return new StorageObjectName(namespace, tableName, optional);
	}

	@Override
	public String getSpec() {
		return spec;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (namespace != null) {
			sb.append(quote(namespace) + ":");
		}
		sb.append(quote(table));
		if (optional)
			sb.append("?");
		return sb.toString();
	}

	@Override
	public List<StorageObjectName> match(LogTableRegistry logTableRegistry) {
		if (pattern == null) {
			return Arrays.asList(new StorageObjectName(namespace, table, optional));
		} else {
			ArrayList<StorageObjectName> result = new ArrayList<StorageObjectName>();
			for (String tableName : logTableRegistry.getTableNames()) {
				if (pattern.matcher(tableName).matches())
					result.add(resolveWith(tableName));
			}
			return result;
		}
	}

	@Override
	public String getNamespace() {
		return namespace;
	}

	@Override
	public String getTable() {
		return table;
	}

	@Override
	public void setTable(String tableName) {
		this.table = tableName;
	}

	@Override
	public void setNamespace(String ns) {
		this.namespace = ns;
	}

	@Override
	public void setOptional(boolean optional) {
		this.optional = optional;
	}
}
