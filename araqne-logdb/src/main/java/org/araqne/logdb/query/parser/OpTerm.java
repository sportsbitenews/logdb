package org.araqne.logdb.query.parser;

import java.util.List;

public interface OpTerm extends Term {
	// like static
	public boolean isInstance(Object o);
	public OpTerm parse(String s);
	public boolean isDelimiter(String s);
	List<OpTerm> delimiters();

	// per instance
	public boolean isUnary();
	public boolean isAlpha();
	public String getSymbol();
	public int getPrecedence();
	public boolean isLeftAssoc();
	public OpTerm postProcessCloseParen(); // Postprocess for last operator before parenthesis closed.
}