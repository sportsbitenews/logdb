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
	private static enum TypeGroup {
		NULL, BOOLEAN, NUM, DATE, IPV4, IPV6, STRING, ARRAY, MAP, BLOB, ELSE
	};

	@Override
	public int compare(Object o1, Object o2) {
		TypeGroup o1TypeGroup = getTypeGroup(o1);
		TypeGroup o2TypeGroup = getTypeGroup(o2);

		int cmpResult = o1TypeGroup.compareTo(o2TypeGroup);
		if (cmpResult == 0) {
			switch (o1TypeGroup) {
			case NULL:
				return 0;
			case NUM:
				return compareNum((Number) o1, (Number) o2);
			case STRING:
				return compareString(o1, o2);
			case ARRAY:
				return compareArray(o1, o2);
			case IPV4:
				return compareIpv4(o1, o2);
			case BOOLEAN:
				return compareBool(o1, o2);
			case DATE:
				return compareDate(o1, o2);
			case IPV6:
				return compareIpv6(o1, o2);
			case MAP:
				return compareMap(o1, o2);
			case BLOB:
				return compareBlob(o1, o2);
			default:
				return compareElse(o1, o2);
			}
		} else {
			return cmpResult;
		}
	}

	private TypeGroup getTypeGroup(Object o) {
		if (o == null)
			return TypeGroup.NULL;
		else if (o instanceof Number)
			return TypeGroup.NUM;
		else if (o instanceof String)
			return TypeGroup.STRING;
		else if (o instanceof Object[])
			return TypeGroup.ARRAY;
		else if (o instanceof Inet4Address)
			return TypeGroup.IPV4;
		else if (o instanceof Boolean)
			return TypeGroup.BOOLEAN;
		else if (o instanceof Date)
			return TypeGroup.DATE;
		else if (o instanceof Inet6Address)
			return TypeGroup.IPV6;
		else if (o instanceof Map)
			return TypeGroup.MAP;
		else if (o instanceof byte[])
			return TypeGroup.BLOB;
		else
			return TypeGroup.ELSE;

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

		int lhs = blob1.length;
		int rhs = blob2.length;
		if (lhs == rhs) {
			return 0;
		} else {
			return lhs < rhs ? -1 : 1;
		}
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

		if (arr1.length == arr2.length) {
			return 0;
		} else {
			return arr1.length < arr2.length ? -1 : 1;
		}
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
			if (ip1[i] != ip2[i]) {
				return ip1[i] < ip2[i] ? -1 : 1;
			}
		}
		return 0;
	}

	private int compareIpv6(Object o1, Object o2) {
		byte[] ip1 = ((Inet6Address) o1).getAddress();
		byte[] ip2 = ((Inet6Address) o2).getAddress();
		for (int i = 0; i < 16; i++) {
			if (ip1[i] != ip2[i]) {
				return ip1[i] < ip2[i] ? -1 : 1;
			}
		}
		return 0;
	}

	private int compareBool(Object o1, Object o2) {
		Boolean bool1 = (Boolean) o1;
		Boolean bool2 = (Boolean) o2;
		return bool1.compareTo(bool2);
	}

	private int compareNum(Number o1, Number o2) {
		if (!NumberUtil.isFloat(o1) && !NumberUtil.isFloat(o2)) {
			long x = o1.longValue();
			long y = o2.longValue();
			return (x < y) ? -1 : ((x == y) ? 0 : 1);
		} else {
			double x = o1.doubleValue();
			double y = o2.doubleValue();
			return Double.compare(x, y);
		}
	}

	private int compareDate(Object o1, Object o2) {
		long lhs = ((Date) o1).getTime();
		long rhs = ((Date) o2).getTime();

		if (lhs == rhs) {
			return 0;
		} else {
			return lhs < rhs ? -1 : 1;
		}
	}
}
