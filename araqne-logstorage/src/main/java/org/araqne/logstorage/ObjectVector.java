package org.araqne.logstorage;

public class ObjectVector implements LogVector {
	
	private Object[] array;
	
	public ObjectVector(Object[] array) {
		this.array = array;
	}

	@Override
	public Object[] getArray() {
		return array;
	}

}
