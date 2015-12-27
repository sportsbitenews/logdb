package org.araqne.logdb;

public class FieldValues {
	public boolean constant;
	public int[] types;
	public long[] longs;
	public double[] doubles;
	public Object[] objs;
	
	public FieldValues(int size) {
		this.types = new int[size];
	}
}
