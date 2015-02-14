/*
 * Copyright 2011 Future Systems
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;

import org.araqne.logdb.query.command.NumberUtil;

public class ObjectComparator implements Comparator<Object> {
	private static enum typeGroup {
		NULL, BOOLEAN, NUM, DATE, IPV4, IPV6, STRING, ARRAY, MAP, BLOB, ELSE
	};

	@Override
	public int compare(Object o1, Object o2) {
		typeGroup o1TypeGroup = getTypeGroup(o1);
		typeGroup o2TypeGroup = getTypeGroup(o2);

		int cmpResult = o1TypeGroup.compareTo(o2TypeGroup);
		if (cmpResult == 0) {
			if (o1TypeGroup == typeGroup.NULL)
				return 0;
			else if (o1TypeGroup == typeGroup.NUM)
				return compareNum(o1, o2);
			else if (o1TypeGroup == typeGroup.STRING)
				return compareString(o1, o2);
			else if (o1TypeGroup == typeGroup.ARRAY)
				return compareArray(o1, o2);
			else if (o1TypeGroup == typeGroup.IPV4)
				return compareIpv4(o1, o2);
			else if (o1TypeGroup == typeGroup.BOOLEAN)
				return compareBool(o1, o2);
			else if (o1TypeGroup == typeGroup.DATE)
				return compareDate(o1, o2);
			else if (o1TypeGroup == typeGroup.IPV6)
				return compareIpv6(o1, o2);
			else if (o1TypeGroup == typeGroup.MAP)
				return compareMap(o1, o2);
			else if (o1TypeGroup == typeGroup.BLOB)
				return compareBlob(o1, o2);
			else
				return compareElse(o1, o2);
		} else {
			return cmpResult;
		}
	}

	private typeGroup getTypeGroup(Object o1) {
		if (o1 == null)
			return typeGroup.NULL;
		else if (o1 instanceof Long || o1 instanceof Integer || o1 instanceof Short || o1 instanceof Double || o1 instanceof Float)
			return typeGroup.NUM;
		else if (o1 instanceof String)
			return typeGroup.STRING;
		else if (o1 instanceof Object[])
			return typeGroup.ARRAY;
		else if (o1 instanceof Inet4Address)
			return typeGroup.IPV4;
		else if (o1 instanceof Boolean)
			return typeGroup.BOOLEAN;
		else if (o1 instanceof Date)
			return typeGroup.DATE;
		else if (o1 instanceof Inet6Address)
			return typeGroup.IPV6;
		else if (o1 instanceof Map)
			return typeGroup.MAP;
		else if (o1 instanceof byte[])
			return typeGroup.BLOB;
		else
			return typeGroup.ELSE;

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private int compareElse(Object o1, Object o2) {
		if (o1 instanceof Comparable) {
			try {
				return ((Comparable) o1).compareTo(o2);
			} catch (Throwable t) {
			}
		}

		String o1ClassName = o1.getClass().getName();
		String o2ClassName = o2.getClass().getName();

		return o1ClassName.compareTo(o2ClassName);
	}

	private int compareBlob(Object o1, Object o2) {
		byte[] blob1 = (byte[]) o1;
		byte[] blob2 = (byte[]) o2;

		int min = Math.min(blob1.length, blob2.length);
		for (int i = 0; i < min; i++) {
			if (blob1[i] < blob2[i])
				return -1;
			else if (blob1[i] > blob2[i])
				return 1;
		}

		return blob1.length - blob2.length;
	}

	private int compareMap(Object o1, Object o2) {
		// Map is not supported.
		// return 0 for all map.
		return 0;
	}

	private int compareArray(Object o1, Object o2) {
		Object[] arr1 = (Object[]) o1;
		Object[] arr2 = (Object[]) o2;

		int min = Math.min(arr1.length, arr2.length);
		for (int i = 0; i < min; i++) {
			int cmp = compare(arr1[i], arr2[i]);
			if (cmp != 0)
				return cmp;
		}

		return arr1.length - arr2.length;
	}

	private int compareString(Object o1, Object o2) {
		String string1 = (String) o1;
		String string2 = (String) o2;

		return string1.compareToIgnoreCase(string2);
	}

	private int compareIpv4(Object o1, Object o2) {
		byte[] ip1 = ((Inet4Address) o1).getAddress();
		byte[] ip2 = ((Inet4Address) o2).getAddress();
		for (int i = 0; i < 4; i++) {
			int c = ip1[i] - ip2[i];
			if (c != 0)
				return c;
		}
		return 0;
	}

	private int compareIpv6(Object o1, Object o2) {
		byte[] ip1 = ((Inet6Address) o1).getAddress();
		byte[] ip2 = ((Inet6Address) o2).getAddress();
		for (int i = 0; i < 16; i++) {
			int c = ip1[i] - ip2[i];
			if (c != 0)
				return c;
		}
		return 0;
	}

	private int compareBool(Object o1, Object o2) {
		Boolean bool1 = (Boolean) o1;
		Boolean bool2 = (Boolean) o2;

		return bool1.compareTo(bool2);
	}

	private int compareNum(Object o1, Object o2) {
		if (!NumberUtil.isFloat(o1) && !NumberUtil.isFloat(o2)) {
			long lhs = 0;
			if (o1 instanceof Long)
				lhs = (Long) o1;
			else if (o1 instanceof Integer)
				lhs = (Integer) o1;
			else if (o1 instanceof Short)
				lhs = (Short) o1;

			long rhs = 0;
			if (o2 instanceof Long)
				rhs = (Long) o2;
			else if (o2 instanceof Integer)
				rhs = (Integer) o2;
			else if (o2 instanceof Short)
				rhs = (Short) o2;

			return (int) (lhs - rhs);
		} else {
			double lhs = 0;
			if (o1 instanceof Double)
				lhs = (Double) o1;
			else if (o1 instanceof Integer)
				lhs = (Integer) o1;
			else if (o1 instanceof Long)
				lhs = (Long) o1;
			else if (o1 instanceof Short)
				lhs = (Short) o1;
			else if (o1 instanceof Float)
				lhs = (Float) o1;

			double rhs = 0;
			if (o2 instanceof Double)
				rhs = (Double) o2;
			else if (o2 instanceof Integer)
				rhs = (Integer) o2;
			else if (o2 instanceof Long)
				rhs = (Long) o2;
			else if (o2 instanceof Short)
				rhs = (Short) o2;
			else if (o2 instanceof Float)
				rhs = (Float) o2;

			return Double.compare(lhs, rhs);
		}

	}

	private int compareDate(Object o1, Object o2) {
		long lhs = ((Date) o1).getTime();
		long rhs = ((Date) o2).getTime();

		return (int) (lhs - rhs);
	}
}
