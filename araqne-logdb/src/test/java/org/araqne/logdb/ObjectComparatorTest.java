package org.araqne.logdb;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ObjectComparatorTest {
	private static Comparator<Object> cmp;

	private static Object NULL1;
	private static Object NULL2;

	private static Boolean BOOL1;
	private static Boolean BOOL2;

	private static Short SHORT1;
	private static Integer INT1;
	private static Long LONG1;
	private static Float FLOAT1;
	private static Double DOUBLE1;

	private static Short SHORT2;
	private static Integer INT2;
	private static Long LONG2;
	private static Float FLOAT2;
	private static Double DOUBLE2;

	private static Date DATE1;
	private static Date DATE2;

	private static Inet4Address IPV4_1;
	private static Inet4Address IPV4_2;

	private static Inet6Address IPV6_1;
	private static Inet6Address IPV6_2;

	private static String STRING1;
	private static String STRING2;

	private static Object APPLE;
	private static Object BANANA;
	private static Object PEAR;
	private static Object TIGER1;
	private static Object TIGER2;

	private Object[] ARRAY;
	private Map<Object, Object> MAP;
	private byte[] BLOB;

	@Before
	public void before() throws UnknownHostException {
		cmp = new ObjectComparator();

		NULL1 = null;
		NULL2 = null;

		BOOL1 = false;
		BOOL2 = true;

		SHORT1 = 0;
		SHORT2 = 1;

		INT1 = 0;
		INT2 = 1;

		LONG1 = (long) 0;
		LONG2 = (long) 1;

		FLOAT1 = 0.0f;
		FLOAT2 = 1.0f;

		DOUBLE1 = 0.0d;
		DOUBLE2 = 1.0d;

		Calendar cal = Calendar.getInstance();
		DATE2 = cal.getTime();
		cal.add(Calendar.DATE, -1);
		DATE1 = cal.getTime();

		DATE1 = cal.getTime();

		IPV4_1 = (Inet4Address) Inet4Address.getByAddress(new byte[] { 10, 0, 0, 1 });
		IPV4_2 = (Inet4Address) Inet4Address.getByAddress(new byte[] { 10, 0, 0, 2 });

		IPV6_1 = (Inet6Address) Inet6Address.getByAddress(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
				15, 16 });
		IPV6_2 = (Inet6Address) Inet6Address.getByAddress(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
				15, 17 });

		STRING1 = "A";
		STRING2 = "B";

		APPLE = new Apple();
		BANANA = new Banana();
		PEAR = new Pear();
		TIGER1 = new Tiger();
		TIGER2 = new Tiger();

		ARRAY = new Object[] { STRING1 };
		MAP = new HashMap<Object, Object>();
		BLOB = new byte[] { 1 };
	}

	@Test
	public void numTypeTest() {
		assertTrue(cmp.compare(SHORT1, SHORT2) < 0);
		assertTrue(cmp.compare(SHORT2, SHORT1) > 0);

		assertTrue(cmp.compare(INT1, INT2) < 0);
		assertTrue(cmp.compare(INT2, INT1) > 0);

		assertTrue(cmp.compare(LONG1, LONG2) < 0);
		assertTrue(cmp.compare(LONG2, LONG1) > 0);

		assertTrue(cmp.compare(FLOAT1, FLOAT2) < 0);
		assertTrue(cmp.compare(FLOAT2, FLOAT1) > 0);

		assertTrue(cmp.compare(DOUBLE1, DOUBLE2) < 0);
		assertTrue(cmp.compare(DOUBLE2, DOUBLE1) > 0);

		assertTrue(cmp.compare(SHORT1, INT2) < 0);
		assertTrue(cmp.compare(INT2, SHORT1) > 0);

		assertTrue(cmp.compare(SHORT1, LONG2) < 0);
		assertTrue(cmp.compare(LONG2, SHORT1) > 0);

		assertTrue(cmp.compare(SHORT1, FLOAT2) < 0);
		assertTrue(cmp.compare(FLOAT2, SHORT1) > 0);

		assertTrue(cmp.compare(SHORT1, DOUBLE2) < 0);
		assertTrue(cmp.compare(DOUBLE2, SHORT1) > 0);

		assertTrue(cmp.compare(INT1, LONG2) < 0);
		assertTrue(cmp.compare(LONG2, INT1) > 0);

		assertTrue(cmp.compare(INT1, FLOAT2) < 0);
		assertTrue(cmp.compare(FLOAT2, INT1) > 0);

		assertTrue(cmp.compare(INT1, DOUBLE2) < 0);
		assertTrue(cmp.compare(DOUBLE2, INT1) > 0);

		assertTrue(cmp.compare(LONG1, FLOAT2) < 0);
		assertTrue(cmp.compare(FLOAT2, LONG1) > 0);

		assertTrue(cmp.compare(LONG1, DOUBLE2) < 0);
		assertTrue(cmp.compare(DOUBLE2, LONG1) > 0);

		assertTrue(cmp.compare(FLOAT1, DOUBLE2) < 0);
		assertTrue(cmp.compare(DOUBLE2, FLOAT1) > 0);
	}

	@Test
	public void nullTypeTest() {
		assertTrue(cmp.compare(NULL1, NULL2) == 0);
		assertTrue(cmp.compare(NULL2, NULL1) == 0);
	}

	@Test
	public void booleanTypeTest() {
		assertTrue(cmp.compare(BOOL1, BOOL2) < 0);
		assertTrue(cmp.compare(BOOL2, BOOL1) > 0);
	}

	@Test
	public void dateTypeTest() {
		assertTrue(cmp.compare(DATE1, DATE2) < 0);
		assertTrue(cmp.compare(DATE2, DATE1) > 0);
	}

	@Test
	public void ipv4TypeTest() {
		assertTrue(cmp.compare(IPV4_1, IPV4_2) < 0);
		assertTrue(cmp.compare(IPV4_2, IPV4_1) > 0);
	}

	public void ipv6TypeTest() {
		assertTrue(cmp.compare(IPV6_1, IPV6_2) < 0);
		assertTrue(cmp.compare(IPV6_2, IPV6_1) > 0);
	}

	@Test
	public void stringTypeTest() {
		assertTrue(cmp.compare(STRING1, STRING2) < 0);
		assertTrue(cmp.compare(STRING2, STRING1) > 0);
	}

	@Test
	public void arrayTypeTest1() {
		Object[] array1 = { STRING1 };
		Object[] array2 = { STRING1, INT1 };
		assertTrue(cmp.compare(array1, array2) < 0);
		assertTrue(cmp.compare(array2, array1) > 0);
	}

	@Test
	public void arrayTypeTest2() {
		Object[] array1 = { STRING1, INT1 };
		Object[] array2 = { STRING1, INT2 };
		assertTrue(cmp.compare(array1, array2) < 0);
		assertTrue(cmp.compare(array2, array1) > 0);
	}

	@Test
	public void arrayTypeTest3() {
		Object[] array1 = { STRING1, INT1 };
		Object[] array2 = { STRING2, INT1 };
		assertTrue(cmp.compare(array1, array2) < 0);
		assertTrue(cmp.compare(array2, array1) > 0);
	}

	@Test
	public void arrayTypeTest4() {
		Object[] array1 = { STRING1, INT1 };
		Object[] array2 = { STRING1, INT1, INT2 };
		assertTrue(cmp.compare(array1, array2) < 0);
		assertTrue(cmp.compare(array2, array1) > 0);
	}

	@Test
	public void arrayTypeTest5() {
		Object[] array1 = { APPLE };
		Object[] array2 = { APPLE };
		assertTrue(cmp.compare(array1, array2) == 0);
		assertTrue(cmp.compare(array2, array1) == 0);
	}

	@Test
	public void arrayTypeTest6() {
		Object[] array1 = { APPLE };
		Object[] array2 = { BANANA };
		assertTrue(cmp.compare(array1, array2) < 0);
		assertTrue(cmp.compare(array2, array1) > 0);
	}

	@Test
	public void arrayTypeTest7() {
		Object[] array1 = { APPLE, STRING1, BANANA };
		Object[] array2 = { APPLE, STRING1, TIGER1 };
		assertTrue(cmp.compare(array1, array2) < 0);
		assertTrue(cmp.compare(array2, array1) > 0);
	}

	@Test
	public void arrayTypeTest8() {
		Object[] array1 = { APPLE, STRING1, TIGER1 };
		Object[] array2 = { APPLE, STRING1, TIGER1, BANANA };
		assertTrue(cmp.compare(array1, array2) < 0);
		assertTrue(cmp.compare(array2, array1) > 0);
	}

	@Test
	public void mapTypeTest1() {
		Map<Object, Object> map1 = new HashMap<Object, Object>();
		Map<Object, Object> map2 = new HashMap<Object, Object>();

		map1.put("a", 0);
		map1.put("b", 0);

		map2.put("a", 0);
		map2.put("b", 1);

		assertTrue(cmp.compare(map1, map2) == 0);
	}

	@Test
	public void blobTypeTest1() {
		byte[] array1 = { 0 };
		byte[] array2 = { 0 };
		assertTrue(cmp.compare(array1, array2) == 0);
		assertTrue(cmp.compare(array2, array1) == 0);
	}

	@Test
	public void blobTypeTest2() {
		byte[] array1 = { 0 };
		byte[] array2 = { 0, 1 };
		assertTrue(cmp.compare(array1, array2) < 0);
		assertTrue(cmp.compare(array2, array1) > 0);
	}

	@Test
	public void blobTypeTest3() {
		byte[] array1 = { 0, 1 };
		byte[] array2 = { 0, 2 };
		assertTrue(cmp.compare(array1, array2) < 0);
		assertTrue(cmp.compare(array2, array1) > 0);
	}

	@Test
	public void blobTypeTest4() {
		byte[] array1 = { 0, 1 };
		byte[] array2 = { 1, 1 };

		assertTrue(cmp.compare(array1, array2) < 0);
		assertTrue(cmp.compare(array2, array1) > 0);
	}

	@Test
	public void blobTypeTest5() {
		byte[] array1 = { 0, 1 };
		byte[] array2 = { 1, 1, 2 };

		assertTrue(cmp.compare(array1, array2) < 0);
		assertTrue(cmp.compare(array2, array1) > 0);
	}

	@Test
	public void elseTypeTest1() {
		assertTrue(cmp.compare(APPLE, BANANA) < 0);
		assertTrue(cmp.compare(BANANA, APPLE) > 0);
	}

	@Test
	public void elseTypeTest2() {
		assertTrue(cmp.compare(APPLE, PEAR) < 0);
		assertTrue(cmp.compare(PEAR, APPLE) > 0);
	}

	@Test
	public void elseTypeTest3() {
		assertTrue(cmp.compare(PEAR, BANANA) < 0);
		assertTrue(cmp.compare(BANANA, PEAR) > 0);
	}

	@Test
	public void elseTypeTest4() {
		assertTrue(cmp.compare(APPLE, APPLE) == 0);
		assertTrue(cmp.compare(BANANA, BANANA) == 0);
		assertTrue(cmp.compare(PEAR, PEAR) == 0);
	}

	@Test
	public void elseTypeTest5() {
		assertTrue(cmp.compare(TIGER1, TIGER2) == 0);
		assertTrue(cmp.compare(TIGER2, TIGER1) == 0);
	}

	@Test
	public void elseTypeTest6() {
		assertTrue(cmp.compare(STRING1, TIGER1) < 0);
		assertTrue(cmp.compare(TIGER1, STRING1) > 0);
	}

	@Test
	public void elseTypeTest7() {
		assertTrue(cmp.compare(BANANA, TIGER1) < 0);
		assertTrue(cmp.compare(TIGER1, BANANA) > 0);
	}

	@Test
	public void nullAndOtherTypeTest() {
		assertTrue(cmp.compare(NULL1, BOOL1) < 0);
		assertTrue(cmp.compare(NULL1, BOOL2) < 0);

		assertTrue(cmp.compare(NULL1, LONG1) < 0);
		assertTrue(cmp.compare(NULL1, LONG2) < 0);

		assertTrue(cmp.compare(NULL1, INT1) < 0);
		assertTrue(cmp.compare(NULL1, INT2) < 0);

		assertTrue(cmp.compare(NULL1, SHORT1) < 0);
		assertTrue(cmp.compare(NULL1, SHORT2) < 0);

		assertTrue(cmp.compare(NULL1, DOUBLE1) < 0);
		assertTrue(cmp.compare(NULL1, DOUBLE2) < 0);

		assertTrue(cmp.compare(NULL1, FLOAT1) < 0);
		assertTrue(cmp.compare(NULL1, FLOAT2) < 0);

		assertTrue(cmp.compare(NULL1, DATE1) < 0);
		assertTrue(cmp.compare(NULL1, DATE2) < 0);

		assertTrue(cmp.compare(NULL1, IPV4_1) < 0);
		assertTrue(cmp.compare(NULL1, IPV4_2) < 0);

		assertTrue(cmp.compare(NULL1, IPV6_1) < 0);
		assertTrue(cmp.compare(NULL1, IPV6_2) < 0);

		assertTrue(cmp.compare(NULL1, STRING1) < 0);
		assertTrue(cmp.compare(NULL1, STRING2) < 0);

		assertTrue(cmp.compare(NULL1, ARRAY) < 0);
		assertTrue(cmp.compare(NULL1, MAP) < 0);
		assertTrue(cmp.compare(NULL1, BLOB) < 0);
	}

	@Test
	public void booleanAndOtherTypeTest() {
		assertTrue(cmp.compare(BOOL1, NULL1) > 0);
		assertTrue(cmp.compare(BOOL1, NULL2) > 0);

		assertTrue(cmp.compare(BOOL1, LONG1) < 0);
		assertTrue(cmp.compare(BOOL1, LONG2) < 0);

		assertTrue(cmp.compare(BOOL1, INT1) < 0);
		assertTrue(cmp.compare(BOOL1, INT2) < 0);

		assertTrue(cmp.compare(BOOL1, SHORT1) < 0);
		assertTrue(cmp.compare(BOOL1, SHORT2) < 0);

		assertTrue(cmp.compare(BOOL1, DOUBLE1) < 0);
		assertTrue(cmp.compare(BOOL1, DOUBLE2) < 0);

		assertTrue(cmp.compare(BOOL1, FLOAT1) < 0);
		assertTrue(cmp.compare(BOOL1, FLOAT2) < 0);

		assertTrue(cmp.compare(BOOL1, DATE1) < 0);
		assertTrue(cmp.compare(BOOL1, DATE2) < 0);

		assertTrue(cmp.compare(BOOL1, IPV4_1) < 0);
		assertTrue(cmp.compare(BOOL1, IPV4_2) < 0);

		assertTrue(cmp.compare(BOOL1, IPV6_1) < 0);
		assertTrue(cmp.compare(BOOL1, IPV6_2) < 0);

		assertTrue(cmp.compare(BOOL1, STRING1) < 0);
		assertTrue(cmp.compare(BOOL1, STRING2) < 0);

		assertTrue(cmp.compare(BOOL1, ARRAY) < 0);
		assertTrue(cmp.compare(BOOL1, MAP) < 0);
		assertTrue(cmp.compare(BOOL1, BLOB) < 0);
	}

	@Test
	public void numAndOtherTypeTest() {
		assertTrue(cmp.compare(LONG1, NULL1) > 0);
		assertTrue(cmp.compare(LONG1, NULL2) > 0);

		assertTrue(cmp.compare(LONG1, BOOL1) > 0);
		assertTrue(cmp.compare(LONG1, BOOL2) > 0);

		assertTrue(cmp.compare(LONG1, DATE1) < 0);
		assertTrue(cmp.compare(LONG1, DATE2) < 0);

		assertTrue(cmp.compare(LONG1, IPV4_1) < 0);
		assertTrue(cmp.compare(LONG1, IPV4_2) < 0);

		assertTrue(cmp.compare(LONG1, IPV6_1) < 0);
		assertTrue(cmp.compare(LONG1, IPV6_2) < 0);

		assertTrue(cmp.compare(LONG1, STRING1) < 0);
		assertTrue(cmp.compare(LONG1, STRING2) < 0);

		assertTrue(cmp.compare(LONG1, ARRAY) < 0);
		assertTrue(cmp.compare(LONG1, MAP) < 0);
		assertTrue(cmp.compare(LONG1, BLOB) < 0);
	}

	@Test
	public void dateAndOtherTypeTest() {
		assertTrue(cmp.compare(DATE1, NULL1) > 0);
		assertTrue(cmp.compare(DATE1, NULL2) > 0);

		assertTrue(cmp.compare(DATE1, BOOL1) > 0);
		assertTrue(cmp.compare(DATE1, BOOL2) > 0);

		assertTrue(cmp.compare(DATE1, LONG1) > 0);
		assertTrue(cmp.compare(DATE1, LONG2) > 0);

		assertTrue(cmp.compare(DATE1, IPV4_1) < 0);
		assertTrue(cmp.compare(DATE1, IPV4_2) < 0);

		assertTrue(cmp.compare(DATE1, IPV6_1) < 0);
		assertTrue(cmp.compare(DATE1, IPV6_2) < 0);

		assertTrue(cmp.compare(DATE1, STRING1) < 0);
		assertTrue(cmp.compare(DATE1, STRING2) < 0);

		assertTrue(cmp.compare(DATE1, ARRAY) < 0);
		assertTrue(cmp.compare(DATE1, MAP) < 0);
		assertTrue(cmp.compare(DATE1, BLOB) < 0);
	}

	@Test
	public void ipv4AndOtherTypeTest() {
		assertTrue(cmp.compare(IPV4_1, NULL1) > 0);
		assertTrue(cmp.compare(IPV4_1, NULL2) > 0);

		assertTrue(cmp.compare(IPV4_1, BOOL1) > 0);
		assertTrue(cmp.compare(IPV4_1, BOOL2) > 0);

		assertTrue(cmp.compare(IPV4_1, LONG1) > 0);
		assertTrue(cmp.compare(IPV4_1, LONG2) > 0);

		assertTrue(cmp.compare(IPV4_1, DATE1) > 0);
		assertTrue(cmp.compare(IPV4_1, DATE2) > 0);

		assertTrue(cmp.compare(IPV4_1, IPV6_1) < 0);
		assertTrue(cmp.compare(IPV4_1, IPV6_2) < 0);

		assertTrue(cmp.compare(IPV4_1, STRING1) < 0);
		assertTrue(cmp.compare(IPV4_1, STRING2) < 0);

		assertTrue(cmp.compare(IPV4_1, ARRAY) < 0);
		assertTrue(cmp.compare(IPV4_1, MAP) < 0);
		assertTrue(cmp.compare(IPV4_1, BLOB) < 0);
	}

	@Test
	public void ipv6AndOtherTypeTest() {
		assertTrue(cmp.compare(IPV6_1, NULL1) > 0);
		assertTrue(cmp.compare(IPV6_1, NULL2) > 0);

		assertTrue(cmp.compare(IPV6_1, BOOL1) > 0);
		assertTrue(cmp.compare(IPV6_1, BOOL2) > 0);

		assertTrue(cmp.compare(IPV6_1, LONG1) > 0);
		assertTrue(cmp.compare(IPV6_1, LONG2) > 0);

		assertTrue(cmp.compare(IPV6_1, DATE1) > 0);
		assertTrue(cmp.compare(IPV6_1, DATE2) > 0);

		assertTrue(cmp.compare(IPV6_1, IPV4_1) > 0);
		assertTrue(cmp.compare(IPV6_1, IPV4_2) > 0);

		assertTrue(cmp.compare(IPV6_1, STRING1) < 0);
		assertTrue(cmp.compare(IPV6_1, STRING2) < 0);

		assertTrue(cmp.compare(IPV6_1, STRING1) < 0);
		assertTrue(cmp.compare(IPV6_1, STRING2) < 0);

		assertTrue(cmp.compare(IPV6_1, ARRAY) < 0);
		assertTrue(cmp.compare(IPV6_1, MAP) < 0);
		assertTrue(cmp.compare(IPV6_1, BLOB) < 0);
	}

	@Test
	public void stringAndOtherTypeTest() {
		assertTrue(cmp.compare(STRING1, NULL1) > 0);
		assertTrue(cmp.compare(STRING1, NULL2) > 0);

		assertTrue(cmp.compare(STRING1, BOOL1) > 0);
		assertTrue(cmp.compare(STRING1, BOOL2) > 0);

		assertTrue(cmp.compare(STRING1, LONG1) > 0);
		assertTrue(cmp.compare(STRING1, LONG2) > 0);

		assertTrue(cmp.compare(STRING1, DATE1) > 0);
		assertTrue(cmp.compare(STRING1, DATE2) > 0);

		assertTrue(cmp.compare(STRING1, IPV4_1) > 0);
		assertTrue(cmp.compare(STRING1, IPV4_2) > 0);

		assertTrue(cmp.compare(STRING1, IPV6_1) > 0);
		assertTrue(cmp.compare(STRING1, IPV6_2) > 0);

		assertTrue(cmp.compare(STRING1, ARRAY) < 0);
		assertTrue(cmp.compare(STRING1, MAP) < 0);
		assertTrue(cmp.compare(STRING1, BLOB) < 0);
	}

	@Test
	public void arrayAndOtherTypeTest() {
		assertTrue(cmp.compare(ARRAY, NULL1) > 0);
		assertTrue(cmp.compare(ARRAY, NULL2) > 0);

		assertTrue(cmp.compare(ARRAY, BOOL1) > 0);
		assertTrue(cmp.compare(ARRAY, BOOL2) > 0);

		assertTrue(cmp.compare(ARRAY, LONG1) > 0);
		assertTrue(cmp.compare(ARRAY, LONG2) > 0);

		assertTrue(cmp.compare(ARRAY, DATE1) > 0);
		assertTrue(cmp.compare(ARRAY, DATE2) > 0);

		assertTrue(cmp.compare(ARRAY, IPV4_1) > 0);
		assertTrue(cmp.compare(ARRAY, IPV4_2) > 0);

		assertTrue(cmp.compare(ARRAY, IPV6_1) > 0);
		assertTrue(cmp.compare(ARRAY, IPV6_2) > 0);

		assertTrue(cmp.compare(ARRAY, STRING1) > 0);
		assertTrue(cmp.compare(ARRAY, MAP) < 0);
		assertTrue(cmp.compare(ARRAY, BLOB) < 0);
	}

	@Test
	public void mapAndOtherTypeTest() {
		assertTrue(cmp.compare(MAP, NULL1) > 0);
		assertTrue(cmp.compare(MAP, NULL2) > 0);

		assertTrue(cmp.compare(MAP, BOOL1) > 0);
		assertTrue(cmp.compare(MAP, BOOL2) > 0);

		assertTrue(cmp.compare(MAP, LONG1) > 0);
		assertTrue(cmp.compare(MAP, LONG2) > 0);

		assertTrue(cmp.compare(MAP, DATE1) > 0);
		assertTrue(cmp.compare(MAP, DATE2) > 0);

		assertTrue(cmp.compare(MAP, IPV4_1) > 0);
		assertTrue(cmp.compare(MAP, IPV4_2) > 0);

		assertTrue(cmp.compare(MAP, IPV6_1) > 0);
		assertTrue(cmp.compare(MAP, IPV6_2) > 0);

		assertTrue(cmp.compare(MAP, STRING1) > 0);
		assertTrue(cmp.compare(MAP, ARRAY) > 0);
		assertTrue(cmp.compare(MAP, BLOB) < 0);
	}

	@Test
	public void blobAndOtherTypeTest() {
		assertTrue(cmp.compare(BLOB, NULL1) > 0);
		assertTrue(cmp.compare(BLOB, NULL2) > 0);

		assertTrue(cmp.compare(BLOB, BOOL1) > 0);
		assertTrue(cmp.compare(BLOB, BOOL2) > 0);

		assertTrue(cmp.compare(BLOB, LONG1) > 0);
		assertTrue(cmp.compare(BLOB, LONG2) > 0);

		assertTrue(cmp.compare(BLOB, DATE1) > 0);
		assertTrue(cmp.compare(BLOB, DATE2) > 0);

		assertTrue(cmp.compare(BLOB, IPV4_1) > 0);
		assertTrue(cmp.compare(BLOB, IPV4_2) > 0);

		assertTrue(cmp.compare(BLOB, IPV6_1) > 0);
		assertTrue(cmp.compare(BLOB, IPV6_2) > 0);

		assertTrue(cmp.compare(BLOB, STRING1) > 0);
		assertTrue(cmp.compare(BLOB, ARRAY) > 0);
		assertTrue(cmp.compare(BLOB, MAP) > 0);
	}

	@SuppressWarnings("rawtypes")
	abstract class Fruit implements Comparable {
		abstract public int getType();

		@Override
		public int compareTo(Object other) {
			if (other instanceof Fruit)
				return this.getType() - ((Fruit) other).getType();
			else
				throw new ClassCastException("WRONG");
		}
	}

	class Apple extends Fruit {
		public int getType() {
			return 1;
		}
	}

	class Pear extends Fruit {
		public int getType() {
			return 2;
		}
	}

	class Banana extends Fruit {
		public int getType() {
			return 3;
		}
	}

	class Tiger {
	}
}