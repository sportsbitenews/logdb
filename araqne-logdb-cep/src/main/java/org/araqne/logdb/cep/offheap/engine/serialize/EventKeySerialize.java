package org.araqne.logdb.cep.offheap.engine.serialize;

import java.nio.ByteBuffer;

import org.araqne.codec.EncodingRule;
import org.araqne.codec.FastEncodingRule;
import org.araqne.logdb.cep.EventKey;

public class EventKeySerialize implements Serialize<EventKey> {
	//public static String delimiter = "-^-";
	private FastEncodingRule enc = new FastEncodingRule();

//	@Override
//	public byte[] serialize(EventKey value) {
//		return marshal(value).array();
//	}
	
	@Override
	public ByteBuffer serialize(EventKey key) {
		String[] array = { key.getTopic(), key.getKey(), key.getHost() };
		return enc.encode(array);
	}

//	@Override
//	public EventKey deserialize(byte[] in, int offset, int length) {
//		// return parse(new String(in, offset, length));
//		return parse(ByteBuffer.wrap(in));
//	}
	
	@Override
	public EventKey deserialize(ByteBuffer bb) {
		Object[] array = EncodingRule.decodeArray(bb);
		return new EventKey((String) array[0], (String) array[1], (String) array[2]);
	}

	@Override
	public int size() {
		return -1;
	}
//
//	public ByteBuffer marshal(EventKey key) {
//		String[] array = { key.getTopic(), key.getKey(), key.getHost() };
//		return enc.encode(array);
//		// ByteBuffer bb = ByteBuffer.allocate(EncodingRule.lengthOf(array));
//		// EncodingRule.encode(bb, array);
//		// return bb;
//		// //
//		// StringBuffer sb = new StringBuffer();
//		// sb.append(key.getTopic());
//		// sb.append(delimiter);
//		// sb.append(key.getKey());
//		// sb.append(delimiter);
//		// if (key.getHost() != null)
//		// sb.append(key.getHost());
//		// return sb.toString();
//	}
//
//	public EventKey parse(ByteBuffer bb) {
//		Object[] array = EncodingRule.decodeArray(bb);
//		return new EventKey((String) array[0], (String) array[1], (String) array[2]);
//		//
//		// String[] parsed = new String[3];
//		//
//		// int i = 0;
//		// int last = 0;
//		// while (true) {
//		// int p = line.indexOf(delimiter, last);
//		// String token = null;
//		// if (p >= 0)
//		// token = line.substring(last, p);
//		// else
//		// token = line.substring(last);
//		//
//		// if (token.isEmpty())
//		// token = null;
//		//
//		// parsed[i] = token;
//		// if (p < 0)
//		// break;
//		//
//		// last = p + delimiter.length();
//		// i++;
//		// }
//		// EventKey evtkey = new EventKey(parsed[0], parsed[1], parsed[2]);
//		// return evtkey;
//	}

}
