package org.araqne.logstorage;

import java.util.Arrays;

public class ObjectVector implements LogVector {

	private Object[] array;

	public ObjectVector(Object[] array) {
		this.array = array;
	}

	public ObjectVector(Object[] array, int size) {
		if (size < array.length)
			this.array = Arrays.copyOfRange(array, 0, size);
		else
			this.array = array;
	}

	@Override
	public Object[] getArray() {
		return array;
	}

}
