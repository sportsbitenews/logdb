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
	/** is there an operator has same symbol */
	public boolean hasAltOp();  
	public OpTerm getAltOp(); 
	public boolean isAlpha();
	public String getSymbol();
	public int getPrecedence();
	public boolean isLeftAssoc();
	/** Postprocess for last operator before parenthesis closed. */
	public OpTerm postProcessCloseParen();
}