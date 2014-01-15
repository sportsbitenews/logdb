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

	public WildcardTableSpec(String spec) {
		this.spec = spec;
		Matcher matcher = qualifierPattern.matcher(spec);
		if (matcher.matches()) {
			namespace = matcher.group(1);
			table = matcher.group(2);
			optional = matcher.group(3) != null;
			pattern = WildcardMatcher.buildPattern(table);
		} else {
			throw new IllegalArgumentException();
		}
	}

	public static Pattern qualifierPattern = Pattern
			.compile("^(?:(`[^`]+`|[\\w\\*]+)\\:|)(`[^`]+`|[\\w\\*]+)(?:(\\?)|)");
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
		return spec;
	}

	@Override
	public List<StorageObjectName> match(LogTableRegistry logTableRegistry) {
		if (pattern == null) {
			return Arrays.asList(new StorageObjectName(namespace, table, optional));
		} else {
			ArrayList<StorageObjectName> result = new ArrayList<StorageObjectName>();
			for (String tableName: logTableRegistry.getTableNames()) {
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
}

