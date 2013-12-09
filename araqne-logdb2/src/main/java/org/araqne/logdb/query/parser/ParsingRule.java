package org.araqne.logdb.query.parser;

public class ParsingRule {
	private OpEmitterFactory oef = null;
	private FuncEmitterFactory fef = null;
	private TermEmitterFactory tef = null;
	private OpTerm opTerm = null;
	
	public OpTerm getOpTerm() {
		return opTerm;
	}

	public void setOpTerm(OpTerm opTerm) {
		if (opTerm == null) {
			throw new IllegalStateException("EmitterFactory cannot be null.");
		}

		this.opTerm = opTerm;
	}

	public ParsingRule(OpTerm op, OpEmitterFactory o, FuncEmitterFactory f, TermEmitterFactory t) {
		if (op == null || o == null || f == null || t == null) {
			throw new IllegalStateException("EmitterFactory cannot be null.");
		}
		
		this.opTerm = op;
		this.oef = o;
		this.fef = f;
		this.tef = t;
	}

	public OpEmitterFactory getOpEmmiterFactory() {
		return oef;
	}

	public void setOpEmmiterFactory(OpEmitterFactory oef) {
		if (oef == null) {
			throw new IllegalStateException("EmitterFactory cannot be null.");
		}

		this.oef = oef;
	}

	public FuncEmitterFactory getFuncEmitterFactory() {
		return fef;
	}

	public void setFuncEmitterFactory(FuncEmitterFactory fef) {
		if (fef == null) {
			throw new IllegalStateException("EmitterFactory cannot be null.");
		}

		this.fef = fef;
	}

	public TermEmitterFactory getTermEmitterFactory() {
		return tef;
	}

	public void setTermEmitterFactory(TermEmitterFactory tef) {
		if (tef == null) {
			throw new IllegalStateException("EmitterFactory cannot be null.");
		}

		this.tef = tef;
	}
}