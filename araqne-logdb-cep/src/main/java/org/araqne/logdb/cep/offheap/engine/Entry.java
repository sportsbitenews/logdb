package org.araqne.logdb.cep.offheap.engine;

import java.nio.ByteBuffer;

import org.araqne.codec.EncodingRule;
import org.araqne.codec.FastEncodingRule;
import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;

public class Entry<K, V> {
	private K key;
	private V value;
	private int hash;
	private long next;
	private long address;
	private long timeoutTime;

	private static FastEncodingRule enc = new FastEncodingRule();
	//
	// public static final int Hash = 0;
	// public static final int Next = 4;
	// public static final int Timeout = 12;
	// public static final int KeyLen = 20;
	// public static final int Key = 24;

	public Entry(K key, V value, int hash, long next, long timeoutTime, long address) {
		this.key = key;
		this.value = value;
		this.hash = hash;
		this.next = next;
		this.address = address;
		this.timeoutTime = timeoutTime;
	}

	public static <K, V> byte[] encode(Entry<K, V> e, int length, Serialize<K> keySerializer,
			Serialize<V> valueSerializer) {
		ByteBuffer bb = ByteBuffer.allocate(length);
		EncodingRule.encodeArray(bb, marshal(e, keySerializer, valueSerializer));
		return bb.array();//
	}
	
	public static <K, V> byte[] encode(Entry<K, V> e, Serialize<K> keySerializer,
			Serialize<V> valueSerializer) {
		return enc.encode(marshal(e, keySerializer, valueSerializer)).array();
	}
	
	public static <K,V> byte[] encode(Object[] marshaled, int length) {
		ByteBuffer bb = ByteBuffer.allocate(length);
		EncodingRule.encodeArray(bb, marshaled);
		return bb.array();//
	}

	public static <K, V> Object[] marshal(Entry<K, V> entry, Serialize<K> keySerializer, Serialize<V> valueSerializer) {
		Object[] array = new Object[5];
		array[0] = entry.getHash();
		array[1] = entry.getNext();
		array[2] = entry.getTimeoutTime();
		array[3] = keySerializer.serialize(entry.getKey()).array();
		array[4] = valueSerializer.serialize(entry.getValue()).array();
		return array;
	}

	public static <K, V> Entry<K, V> decode(byte[] bytes, Serialize<K> keySerializer, Serialize<V> valueSerializer,
			long address) {
		Object[] array = EncodingRule.decodeArray(ByteBuffer.wrap(bytes));
		return parse(array, keySerializer, valueSerializer, address);
	}

	public static <K, V> Entry<K, V> parse(Object[] array, Serialize<K> keySerializer, Serialize<V> valueSerializer,
			long address) {
		int hash = (Integer) array[0];
		long next = (Long) array[1];
		long timeoutTime = (Long) array[2];
		byte[] keyBytes = (byte[]) array[3];
		byte[] valueBytes = (byte[]) array[4];
		K key = keySerializer.deserialize(ByteBuffer.wrap(keyBytes));
		V value = valueSerializer.deserialize(ByteBuffer.wrap(valueBytes));
		return new Entry<K, V>(key, value, hash, next, timeoutTime, address);
	}

	public static int lengthOf(Object[] list) {
		return EncodingRule.lengthOf(list);
	}

	public int lengthOf(Serialize<K> keySerializer, Serialize<V> valueSerializer) {
		return lengthOf(marshal(this, keySerializer, valueSerializer));
	}

	// public static int sizeOf(int keyLength, int valueLength) {
	// return Key + keyLength + valueLength;
	// }

	// public static <K, V> byte[] encode(Entry<K, V> e, Serialize<K>
	// keySerializer, Serialize<V> valueSerializer) {
	// byte[] keyBytes = keySerializer.serialize(e.getKey());
	// int keyLen = keySerializer.size() == -1 ? keyBytes.length :
	// keySerializer.size();
	// byte[] valueBytes = valueSerializer.serialize(e.getValue());
	// int valueLen = valueSerializer.size() == -1 ? valueBytes.length :
	// keySerializer.size();
	// int dataSize = Entry.Key + keyLen + valueLen;
	// return marshal(dataSize, e.getHash(), e.getNext(), e.getTimeoutTime(),
	// keyLen, keyBytes, valueBytes);
	// }

	// @Deprecated
	// public static <K, V> byte[] encode(Entry<K, V> e, byte[] keyByte, byte[]
	// valueByte) {
	// int keyLen = keyByte.length;
	// int valueLen = valueByte.length;
	// int dataSize = sizeOf(keyLen, valueLen);
	// return marshal(dataSize, e.getHash(), e.getNext(), e.getTimeoutTime(),
	// keyLen, keyByte, valueByte);
	// }
	//
	// private static byte[] marshal(int dataSize, int hash, long next, long
	// timeoutTime, int keyLen, byte[] keyBytes,
	// byte[] valueBytes) {
	// byte[] dataBytes = new byte[dataSize];
	// System.arraycopy(toBytes(hash), 0, dataBytes, Entry.Hash, 4);
	// System.arraycopy(toBytes(next), 0, dataBytes, Entry.Next, 8);
	// System.arraycopy(toBytes(timeoutTime), 0, dataBytes, Entry.Timeout, 8);
	// System.arraycopy(toBytes(keyLen), 0, dataBytes, Entry.KeyLen, 4);
	// System.arraycopy(keyBytes, 0, dataBytes, Entry.Key, keyBytes.length);
	// System.arraycopy(valueBytes, 0, dataBytes, Entry.Key + keyBytes.length,
	// valueBytes.length);
	// return dataBytes;
	// }
	//
	// // @Deprecated
	// public static <K, V> byte[] encodeEntry(Entry<K, V> entry, Serialize<K>
	// keySerializer, Serialize<V> valueSerializer) {
	// Object[] array = Entry.marshal(entry, keySerializer, valueSerializer);
	// ByteBuffer bb = ByteBuffer.allocate(Entry.lengthOf(array));
	// EncodingRule.encode(bb, array);
	// return bb.array();
	// }
	//
	// public static <K, V> byte[] encodeEntry(Object[] array) {
	// ByteBuffer bb = ByteBuffer.allocate(Entry.lengthOf(array));
	// EncodingRule.encode(bb, array);
	// return bb.array();
	// }
	//
	// @Deprecated
	// public static <K, V> Entry<K, V> decode(byte[] value, Serialize<K>
	// keySerializer, Serialize<V> valueSerializer,
	// long address) {
	// if (value == null || value.length == 0)
	// return null;
	//
	// int hash = toInt(value, Entry.Hash);
	// long next = toLong(value, Entry.Next);
	// long timeoutTime = toLong(value, Entry.Timeout);
	// int keyLen = toInt(value, Entry.KeyLen);
	// K k = keySerializer.deserialize(value, Entry.Key, keyLen);
	// V v = valueSerializer.deserialize(value, Entry.Key + keyLen, value.length
	// - (Entry.Key + keyLen));
	// return new Entry<K, V>(k, v, hash, next, timeoutTime, address);
	// }
	//
	// // @Deprecated
	// public static <K, V> Entry<K, V> decodeEntry(byte[] bytes, Serialize<K>
	// keySerializer,
	// Serialize<V> valueSerializer, long address) {
	// Object[] list = (Object[]) EncodingRule.decode(ByteBuffer.wrap(bytes));
	// return parse(list, keySerializer, valueSerializer, address);
	// }

	public int getHash() {
		return hash;
	}

	public void setHash(int hash) {
		this.hash = hash;
	}

	public K getKey() {
		return key;
	}

	public V getValue() {
		return value;
	}

	public V setValue(V v) {
		V oldValue = value;
		value = v;
		return oldValue;
	}

	public boolean equalsKey(Object k) {
		// if (!(object instanceof String))
		// return false;
		// XXX: 성능향상을 위해 hash 값 부터 비교? <- 이걸 밖으로 빼야되나... -> 빼야될듯? 아니면 hash알고리즘을
		// 쓸수있게하거나

		return k.equals(key);
	}

	public long getNext() {
		return next;
	}

	public void setNext(long next) {
		this.next = next;
	}

	public void setAddress(long address) {
		this.address = address;
	}

	public long getAddress() {
		return address;
	}

	public long getTimeoutTime() {
		return timeoutTime;
	}

	public void setTimeoutTime(long timeoutTime) {
		this.timeoutTime = timeoutTime;
	}

	public String toString() {
		return "key = " + key + ", value = " + value + ", hash = " + hash + ", address = " + address
				+ ", next address = " + next + ", timeout time = " + timeoutTime;
	}

	// public static int toInt(byte bytes[], int s) {
	// return ((((int) bytes[s + 0] & 0xff) << 24) | (((int) bytes[s + 1] &
	// 0xff) << 16)
	// | (((int) bytes[s + 2] & 0xff) << 8) | (((int) bytes[s + 3] & 0xff)));
	// }
	//
	// public static long toLong(byte[] data, int s) {
	// if (data == null || data.length < 8 + s)
	// return 0x0;
	//
	// return (long) ((long) (0xff & data[0 + s]) << 56 | (long) (0xff & data[1
	// + s]) << 48
	// | (long) (0xff & data[2 + s]) << 40 | (long) (0xff & data[3 + s]) << 32
	// | (long) (0xff & data[4 + s]) << 24 | (long) (0xff & data[5 + s]) << 16
	// | (long) (0xff & data[6 + s]) << 8 | (long) (0xff & data[7 + s]) << 0);
	// }
	//
	// public static byte[] toBytes(int value) {
	// byte[] byteArray = new byte[4];
	// byteArray[0] = (byte) (value >> 24);
	// byteArray[1] = (byte) (value >> 16);
	// byteArray[2] = (byte) (value >> 8);
	// byteArray[3] = (byte) (value);
	// return byteArray;
	// }
	//
	// public static byte[] toBytes(long value) {
	// return new byte[] { (byte) (value >> 56), (byte) (value >> 48), (byte)
	// (value >> 40), (byte) (value >> 32),
	// (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte)
	// value };
	// }

	// public K parseKey(byte[] values, Serialize<K> keySerializer) {
	//
	// int keyLen = toInt(values, KeyLen);
	// return keySerializer.deserialize(values, Key, keyLen);
	// }
	//
	// public K parseKey(byte[] values, Serialize<K> keySerializer) {
	//
	// int keyLen = toInt(values, KeyLen);
	// return keySerializer.deserialize(values, Key, keyLen);
	// }

}
