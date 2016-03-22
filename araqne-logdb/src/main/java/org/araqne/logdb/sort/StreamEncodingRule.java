/*
 * Copyright 2016 EEDIOM Inc.
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
package org.araqne.logdb.sort;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.codec.TypeMismatchException;
import org.araqne.codec.UnsupportedTypeException;

public class StreamEncodingRule {
	public static final byte NULL_TYPE = 0;
	public static final byte BOOLEAN_TYPE = 1;
	// INT16_TYPE, INT32_TYPE, INT64_TYPE are not used after 2.0
	public static final byte INT16_TYPE = 2;
	public static final byte INT32_TYPE = 3;
	public static final byte INT64_TYPE = 4;
	public static final byte STRING_TYPE = 5; // utf-8 only
	public static final byte DATE_TYPE = 6;
	public static final byte IP4_TYPE = 7;
	public static final byte IP6_TYPE = 8;
	public static final byte MAP_TYPE = 9;
	public static final byte ARRAY_TYPE = 10;
	public static final byte BLOB_TYPE = 11;
	public static final byte ZINT16_TYPE = 12;
	public static final byte ZINT32_TYPE = 13;
	public static final byte ZINT64_TYPE = 14;
	public static final byte FLOAT_TYPE = 15;
	public static final byte DOUBLE_TYPE = 16;
	public static final byte SMAP_TYPE = 17;
	public static final byte SARRAY_TYPE = 18;

	private StreamEncodingRule() {
	}

	@SuppressWarnings("unchecked")
	public static void encode(OutputStream os, Object value) throws IOException {
		if (value == null) {
			encodeNull(os);
		} else if (value instanceof String) {
			encodeString(os, (String) value);
		} else if (value instanceof Long) {
			encodeLong(os, (Long) value);
		} else if (value instanceof Integer) {
			encodeInt(os, (Integer) value);
		} else if (value instanceof Short) {
			encodeShort(os, (Short) value);
		} else if (value instanceof Date) {
			encodeDate(os, (Date) value);
		} else if (value instanceof Inet4Address) {
			encodeIp4(os, (Inet4Address) value);
		} else if (value instanceof Inet6Address) {
			encodeIp6(os, (Inet6Address) value);
		} else if (value instanceof Map<?, ?>) {
			encodeMap(os, (Map<String, Object>) value);
		} else if (value instanceof List<?>) {
			encodeArray(os, (List<?>) value);
		} else if (value.getClass().isArray()) {
			Class<?> c = value.getClass().getComponentType();
			if (c == Object.class) {
				encodeArray(os, Arrays.asList((Object[]) value));
			} else if (c == byte.class) {
				encodeBlob(os, (byte[]) value);
			} else if (c == int.class) {
				encodeArray(os, (int[]) value);
			} else if (c == long.class) {
				encodeArray(os, (long[]) value);
			} else if (c == short.class) {
				encodeArray(os, (short[]) value);
			} else if (c == boolean.class) {
				encodeArray(os, (boolean[]) value);
			} else if (c == double.class) {
				encodeArray(os, (double[]) value);
			} else if (c == float.class) {
				encodeArray(os, (float[]) value);
			} else if (c == char.class) {
				throw new UnsupportedTypeException(value.getClass().getName());
			}
		} else if (value instanceof Boolean) {
			encodeBoolean(os, (Boolean) value);
		} else if (value instanceof Float) {
			encodeFloat(os, (Float) value);
		} else if (value instanceof Double) {
			encodeDouble(os, (Double) value);
		} else {
			throw new UnsupportedTypeException(value.getClass().getName());
		}
	}

	@SuppressWarnings("unchecked")
	public static int lengthOf(Object value) {
		if (value == null) {
			return lengthOfNull();
		} else if (value instanceof String) {
			return lengthOfString((String) value);
		} else if (value instanceof Long) {
			return lengthOfLong((Long) value);
		} else if (value instanceof Integer) {
			return lengthOfInt((Integer) value);
		} else if (value instanceof Short) {
			return lengthOfShort((Short) value);
		} else if (value instanceof Date) {
			return lengthOfDate();
		} else if (value instanceof Inet4Address) {
			return lengthOfIp4((Inet4Address) value);
		} else if (value instanceof Inet6Address) {
			return lengthOfIp6((Inet6Address) value);
		} else if (value instanceof Map<?, ?>) {
			return lengthOfMap((Map<String, Object>) value);
		} else if (value instanceof List<?>) {
			return lengthOfArray((List<?>) value);
		} else if (value.getClass().isArray()) {
			Class<?> c = value.getClass().getComponentType();
			if (c == byte.class) {
				return lengthOfBlob((byte[]) value);
			} else if (c == int.class) {
				return lengthOfArray((int[]) value);
			} else if (c == long.class) {
				return lengthOfArray((long[]) value);
			} else if (c == short.class) {
				return lengthOfArray((short[]) value);
			} else if (c == boolean.class) {
				return lengthOfArray((boolean[]) value);
			} else if (c == double.class) {
				return lengthOfArray((double[]) value);
			} else if (c == float.class) {
				return lengthOfArray((float[]) value);
			} else if (c == char.class) {
				throw new UnsupportedTypeException(value.getClass().getName());
			} else {
				return lengthOfArray((Object[]) value);
			}
		} else if (value instanceof Boolean) {
			return lengthOfBoolean((Boolean) value);
		} else if (value instanceof Float) {
			return lengthOfFloat((Float) value);
		} else if (value instanceof Double) {
			return lengthOfDouble((Double) value);
		} else {
			throw new UnsupportedTypeException(value.getClass().getName());
		}
	}

	public static Object decode(InputStream is) throws IOException {
		int typeByte = is.read();
		switch (typeByte) {
		case NULL_TYPE:
			return null;
		case STRING_TYPE:
			return decodeString(is);
		case INT32_TYPE:
			throw new UnsupportedTypeException("deprecated number type");
		case INT16_TYPE:
			throw new UnsupportedTypeException("deprecated number type");
		case INT64_TYPE:
			throw new UnsupportedTypeException("deprecated number type");
		case DATE_TYPE:
			return decodeDate(is);
		case IP4_TYPE:
			return decodeIp4(is);
		case IP6_TYPE:
			return decodeIp6(is);
		case SMAP_TYPE:
			return decodeMap(is);
		case SARRAY_TYPE:
			return decodeArray(is);
		case BLOB_TYPE:
			return decodeBlob(is);
		case BOOLEAN_TYPE:
			return decodeBoolean(is);
		case ZINT32_TYPE:
			return (int) decodeInt(is);
		case ZINT16_TYPE:
			return (short) decodeShort(is);
		case ZINT64_TYPE:
			return (long) decodeLong(is);
		case FLOAT_TYPE:
			return (float) decodeFloat(is);
		case DOUBLE_TYPE:
			return (double) decodeDouble(is);
		}

		throw new UnsupportedTypeException("type: " + typeByte);
	}

	public static void encodeNull(OutputStream os) throws IOException {
		os.write(NULL_TYPE);
	}

	public static void encodeNumber(OutputStream os, Class<?> clazz, long value) throws IOException {
		if (clazz.equals(int.class)) {
			encodeInt(os, (int) value);
		} else if (clazz.equals(long.class)) {
			encodeLong(os, value);
		} else if (clazz.equals(short.class)) {
			encodeShort(os, (short) value);
		} else {
			throw new UnsupportedTypeException("invalid number type: " + clazz.getName());
		}
	}

	public static void encodeLength(OutputStream os, int value) throws IOException {
		if (value <= 127) {
			os.write(value);
		} else if (value <= 16383) {
			os.write(0x80 | ((value >> 7) & 0x7f));
			os.write(value & 0x7f);
		} else {
			int count = (63 - Long.numberOfLeadingZeros(value)) / 7 + 1;
			for (int i = 0; i < count; ++i) {
				byte signalBit = (byte) (i != count - 1 ? 0x80 : 0);
				byte data = (byte) (signalBit | (byte) (value >> (7 * (count - i - 1)) & 0x7F));
				os.write(data);
			}
		}
	}

	public static void encodeRawNumber(OutputStream os, Class<?> clazz, long value) throws IOException {
		int len = lengthOfRawNumber(clazz, value);
		for (int i = 0; i < len; ++i) {
			byte signalBit = (byte) (i != len - 1 ? 0x80 : 0);
			byte data = (byte) (signalBit | (byte) (value >> (7 * (len - i - 1)) & 0x7F));
			os.write(data);
		}
	}

	public static long decodeRawNumber(InputStream is) throws IOException {
		long value = 0L;

		byte b;
		do {
			value = value << 7;
			b = (byte) is.read();
			value |= b & 0x7F;
		} while ((b & 0x80) == 0x80);
		return value;
	}

	public static void encodePlainLong(OutputStream os, long value) throws IOException {
		os.write(INT64_TYPE);
		encodeRawNumber(os, long.class, value);
	}

	public static long decodePlainLong(InputStream is) throws IOException {
		byte type = (byte) is.read();
		if (type != INT64_TYPE)
			throw new TypeMismatchException(INT64_TYPE, type, -1);

		return (long) decodeRawNumber(is);
	}

	public static void encodePlainInt(OutputStream os, int value) throws IOException {
		os.write(INT32_TYPE);
		encodeRawNumber(os, int.class, value);
	}

	public static int decodePlainInt(InputStream is) throws IOException {
		byte type = (byte) is.read();
		if (type != INT32_TYPE)
			throw new TypeMismatchException(INT32_TYPE, type, -1);

		return (int) decodeRawNumber(is);
	}

	public static void encodePlainShort(OutputStream os, short value) throws IOException {
		os.write(INT16_TYPE);
		encodeRawNumber(os, short.class, value);
	}

	public static short decodePlainShort(InputStream is) throws IOException {
		byte type = (byte) is.read();
		if (type != INT16_TYPE)
			throw new TypeMismatchException(INT16_TYPE, type, -1);

		return (short) decodeRawNumber(is);
	}

	public static void encodeLong(OutputStream os, long value) throws IOException {
		os.write(ZINT64_TYPE);
		long zvalue = (value << 1) ^ (value >> 63);
		encodeRawNumber(os, long.class, zvalue);
	}

	private static long decodeLong(InputStream is) throws IOException {
		long zvalue = (long) decodeRawNumber(is);
		return ((zvalue >> 1) & 0x7FFFFFFFFFFFFFFFL) ^ -(zvalue & 1);
	}

	public static void encodeInt(OutputStream os, int value) throws IOException {
		os.write(ZINT32_TYPE);
		long zvalue = ((long) value << 1) ^ ((long) value >> 31);
		encodeRawNumber(os, int.class, zvalue);
	}

	private static int decodeInt(InputStream is) throws IOException {
		int zvalue = (int) decodeRawNumber(is);
		int v = (int) (((zvalue >> 1) & 0x7FFFFFFF) ^ -(zvalue & 1));
		return v;
	}

	public static void encodeShort(OutputStream os, short value) throws IOException {
		os.write(ZINT16_TYPE);
		long zvalue = ((long) value << 1) ^ ((long) value >> 15);
		encodeRawNumber(os, short.class, zvalue);
	}

	private static short decodeShort(InputStream is) throws IOException {
		long zvalue = decodeRawNumber(is);
		return (short) (((zvalue >> 1) & 0x7FFF) ^ -(zvalue & 1));
	}

	public static void encodeString(OutputStream os, String value) throws IOException {
		os.write(STRING_TYPE);
		try {
			byte[] buffer = value.getBytes("utf-8");
			encodeLength(os, buffer.length);
			os.write(buffer);
		} catch (UnsupportedEncodingException e) {
		}
	}

	private static String decodeString(InputStream is) throws IOException {
		int length = (int) decodeRawNumber(is);
		byte[] b = new byte[length];
		IoHelper.ensureRead(is, b, length);

		return new String(b, "utf-8");
	}

	public static void encodeDate(OutputStream os, Date value) throws IOException {
		long l = value.getTime();
		byte[] b = new byte[9];
		b[0] = DATE_TYPE;
		b[1] = (byte) ((l >> 56) & 0xff);
		b[2] = (byte) ((l >> 48) & 0xff);
		b[3] = (byte) ((l >> 40) & 0xff);
		b[4] = (byte) ((l >> 32) & 0xff);
		b[5] = (byte) ((l >> 24) & 0xff);
		b[6] = (byte) ((l >> 16) & 0xff);
		b[7] = (byte) ((l >> 8) & 0xff);
		b[8] = (byte) (l & 0xff);
		os.write(b);
	}

	private static Date decodeDate(InputStream is) throws IOException {
		byte[] b = new byte[8];
		IoHelper.ensureRead(is, b, 8);
		long l = 0;
		l |= b[0] << 56;
		l |= b[0] << 48;
		l |= b[0] << 40;
		l |= b[0] << 32;
		l |= b[0] << 24;
		l |= b[0] << 16;
		l |= b[0] << 8;
		l |= b[0];
		return new Date(l);
	}

	public static void encodeBoolean(OutputStream os, boolean value) throws IOException {
		os.write(BOOLEAN_TYPE);
		os.write((byte) (value ? 1 : 0));
	}

	private static boolean decodeBoolean(InputStream is) throws IOException {
		byte value = (byte) is.read();
		return value == 1;
	}

	public static void encodeIp4(OutputStream os, Inet4Address value) throws IOException {
		os.write(IP4_TYPE);
		os.write(value.getAddress());
	}

	private static InetAddress decodeIp4(InputStream is) throws IOException {
		byte[] address = new byte[4];
		IoHelper.ensureRead(is, address, 4);
		try {
			return Inet4Address.getByAddress(address);
		} catch (UnknownHostException e) {
			// bytes always correct. ignore.
			return null;
		}
	}

	public static void encodeIp6(OutputStream os, Inet6Address value) throws IOException {
		os.write(IP6_TYPE);
		os.write(value.getAddress());
	}

	private static InetAddress decodeIp6(InputStream is) throws IOException {
		byte[] address = new byte[16];
		IoHelper.ensureRead(is, address, 16);
		try {
			return Inet6Address.getByAddress(address);
		} catch (UnknownHostException e) {
			// bytes always correct. ignore.
			return null;
		}
	}

	public static void encodeMap(OutputStream os, Map<String, Object> map) throws IOException {
		os.write(SMAP_TYPE);

		encodeRawNumber(os, int.class, map.size());

		for (String key : map.keySet()) {
			os.write(STRING_TYPE);
			byte[] keyBytes = key.getBytes("utf-8");
			encodeLength(os, keyBytes.length);
			os.write(keyBytes);

			Object value = map.get(key);
			if (value instanceof String) {
				byte[] valueBytes = ((String) value).getBytes("utf-8");
				os.write(STRING_TYPE);
				encodeLength(os, valueBytes.length);
				os.write(valueBytes);
			} else
				encode(os, value);
		}
	}

	private static Map<String, Object> decodeMap(InputStream is) throws IOException {
		int count = (int) decodeRawNumber(is);

		HashMap<String, Object> m = new HashMap<String, Object>();

		while (count > 0) {
			// parse key
			byte ktype = (byte) is.read();
			if (ktype != STRING_TYPE)
				throw new TypeMismatchException(STRING_TYPE, ktype, -1);

			int klength = (int) decodeRawNumber(is);
			byte[] b = new byte[klength];
			IoHelper.ensureRead(is, b, klength);
			String key = new String(b, "utf-8");
			Object value = decode(is);

			m.put(key, value);
			count--;
		}

		return m;
	}

	public static void encodeArray(OutputStream os, List<?> array) throws IOException {
		os.write(SARRAY_TYPE);

		int length = array.size();
		encodeLength(os, length);

		for (Object obj : array) {
			if (obj instanceof String) {
				byte[] b = ((String) obj).getBytes("utf-8");
				os.write(STRING_TYPE);
				encodeLength(os, b.length);
				os.write(b);
			} else
				encode(os, obj);
		}
	}

	public static void encodeArray(OutputStream os, int[] array) throws IOException {
		os.write(SARRAY_TYPE);
		encodeLength(os, array.length);

		for (int i : array)
			encodeInt(os, i);
	}

	public static void encodeArray(OutputStream os, long[] array) throws IOException {
		os.write(SARRAY_TYPE);
		encodeLength(os, array.length);

		for (long i : array)
			encodeLong(os, i);
	}

	public static void encodeArray(OutputStream os, short[] array) throws IOException {
		os.write(SARRAY_TYPE);
		encodeLength(os, array.length);

		for (short i : array)
			encodeShort(os, i);
	}

	public static void encodeArray(OutputStream os, double[] array) throws IOException {
		os.write(SARRAY_TYPE);
		encodeLength(os, array.length);
		
		for (double i : array)
			encodeDouble(os, i);
	}

	public static void encodeArray(OutputStream os, float[] array) throws IOException {
		os.write(SARRAY_TYPE);
		encodeLength(os, array.length);
		
		for (float i : array)
			encodeFloat(os, i);
	}

	public static void encodeArray(OutputStream os, boolean[] array) throws IOException {
		os.write(ARRAY_TYPE);
		encodeLength(os, array.length);
		
		for (boolean i : array)
			encodeBoolean(os, i);
	}

	public static void encodeArray(OutputStream os, Object[] array) throws IOException {
		encodeArray(os, Arrays.asList(array));
	}

	private static Object[] decodeArray(InputStream is) throws IOException {
		int length = (int) decodeRawNumber(is);

		ArrayList<Object> l = new ArrayList<Object>();
		while (length > 0) {
			l.add(decode(is));
			length--;
		}

		return l.toArray();
	}

	public static void encodeBlob(OutputStream os, byte[] buffer) throws IOException {
		os.write(BLOB_TYPE);
		encodeLength(os, buffer.length);
		os.write(buffer);
	}

	private static byte[] decodeBlob(InputStream is) throws IOException {
		int length = (int) decodeRawNumber(is);
		byte[] blob = new byte[length];
		IoHelper.ensureRead(is, blob, length);
		return blob;
	}

	public static void encodeFloat(OutputStream os, float value) throws IOException {
		os.write(FLOAT_TYPE);
		int v = Float.floatToIntBits(value);
		byte[] b = new byte[4];
		for (int i = 3; i >= 0; i--) {
			b[i] = (byte) (v & 0xFF);
			v >>= 8;
		}
		os.write(b);
	}

	private static float decodeFloat(InputStream is) throws IOException {
		byte[] b = new byte[4];
		IoHelper.ensureRead(is, b, 4);
		int v = 0;
		for (int i = 0; i < 4; i++) {
			v <<= 8;
			v |= b[i] & 0xFF;
		}
		return Float.intBitsToFloat(v);
	}

	public static void encodeDouble(OutputStream os, double value) throws IOException {
		os.write(DOUBLE_TYPE);
		long v = Double.doubleToLongBits(value);
		byte[] b = new byte[8];
		for (int i = 7; i >= 0; i--) {
			b[i] = (byte) (v & 0xFF);
			v >>= 8;
		}
		os.write(b);
	}

	private static double decodeDouble(InputStream is) throws IOException {
		byte[] b = new byte[8];
		IoHelper.ensureRead(is, b, 8);
		long v = 0;
		for (int i = 0; i < 8; i++) {
			v <<= 8;
			v |= b[i] & 0xFF;
		}
		return Double.longBitsToDouble(v);
	}

	public static int lengthOfLong(long value) {
		long zvalue = (value << 1) ^ (value >> 63);
		return 1 + lengthOfRawNumber(long.class, zvalue);
	}

	public static <T> int lengthOfRawNumber(Class<T> clazz, long value) {
		if (value < 0) {
			if (long.class == clazz)
				return 10; // max length for long
			else if (int.class == clazz)
				return 5; // max length for int
			else
				return 3; // max length for short
		} else {
			if (value <= 127)
				return 1;
			if (value <= 16383)
				return 2;
		}

		return (63 - Long.numberOfLeadingZeros(value)) / 7 + 1;
	}

	public static <T> int lengthOfNumber(Class<T> clazz, long value) {
		if (clazz.equals(int.class)) {
			return lengthOfInt((int) value);
		} else if (clazz.equals(long.class)) {
			return lengthOfLong(value);
		} else if (clazz.equals(short.class)) {
			return lengthOfShort((short) value);
		} else {
			throw new UnsupportedTypeException("invalid number type: " + clazz.getName());
		}
	}

	public static int lengthOfInt(int value) {
		int zvalue = (value << 1) ^ (value >> 31);
		return 1 + lengthOfRawNumber(int.class, zvalue);
	}

	public static int lengthOfNull() {
		return 1;
	}

	public static int lengthOfShort(short value) {
		short zvalue = (short) ((value << 1) ^ (value >> 15));
		return 1 + lengthOfRawNumber(short.class, zvalue);
	}

	public static int lengthOfString(String value) {
		byte[] buffer = null;
		try {
			buffer = value.getBytes("utf-8");
		} catch (UnsupportedEncodingException e) {
		}
		return 1 + lengthOfRawNumber(int.class, buffer.length) + buffer.length;
	}

	public static int lengthOfDate() {
		return 1 + 8;
	}

	public static int lengthOfBoolean(boolean value) {
		return 2;
	}

	public static int lengthOfIp4(Inet4Address value) {
		return 1 + value.getAddress().length;
	}

	public static int lengthOfIp6(Inet6Address value) {
		return 1 + value.getAddress().length;
	}

	public static int lengthOfMap(Map<String, Object> value) {
		int contentLength = 0;
		for (String key : value.keySet()) {
			contentLength += lengthOfString(key);
			contentLength += lengthOf(value.get(key));
		}
		return 1 + lengthOfRawNumber(int.class, contentLength) + contentLength;
	}

	public static int lengthOfArray(int[] value) {
		int contentLength = 0;
		for (int obj : value) {
			contentLength += lengthOfInt(obj);
		}
		return 1 + lengthOfRawNumber(int.class, contentLength) + contentLength;
	}

	public static int lengthOfArray(long[] value) {
		int contentLength = 0;
		for (long obj : value) {
			contentLength += lengthOfLong(obj);
		}
		return 1 + lengthOfRawNumber(int.class, contentLength) + contentLength;
	}

	public static int lengthOfArray(short[] value) {
		int contentLength = 0;
		for (short obj : value) {
			contentLength += lengthOfShort(obj);
		}
		return 1 + lengthOfRawNumber(int.class, contentLength) + contentLength;
	}

	public static int lengthOfArray(boolean[] value) {
		int contentLength = 0;
		for (boolean obj : value) {
			contentLength += lengthOfBoolean(obj);
		}
		return 1 + lengthOfRawNumber(int.class, contentLength) + contentLength;
	}

	public static int lengthOfArray(double[] value) {
		int contentLength = 0;
		for (double obj : value) {
			contentLength += lengthOfDouble(obj);
		}
		return 1 + lengthOfRawNumber(int.class, contentLength) + contentLength;
	}

	public static int lengthOfArray(float[] value) {
		int contentLength = 0;
		for (float obj : value) {
			contentLength += lengthOfFloat(obj);
		}
		return 1 + lengthOfRawNumber(int.class, contentLength) + contentLength;
	}

	public static int lengthOfArray(List<?> value) {
		int contentLength = 0;
		for (Object obj : value) {
			contentLength += lengthOf(obj);
		}
		return 1 + lengthOfRawNumber(int.class, contentLength) + contentLength;
	}

	public static int lengthOfArray(Object[] value) {
		return lengthOfArray(Arrays.asList(value));
	}

	public static int lengthOfBlob(byte[] value) {
		return 1 + lengthOfRawNumber(int.class, value.length) + value.length;
	}

	public static int lengthOfFloat(float value) {
		return 1 + 4;
	}

	public static int lengthOfDouble(double value) {
		return 1 + 8;
	}
}
