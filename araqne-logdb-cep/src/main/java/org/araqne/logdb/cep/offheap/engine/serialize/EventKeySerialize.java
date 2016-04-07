package org.araqne.logdb.cep.offheap.engine.serialize;

import java.nio.ByteBuffer;

import org.araqne.codec.EncodingRule;
import org.araqne.codec.FastEncodingRule;
import org.araqne.logdb.cep.EventKey;

public class EventKeySerialize implements Serialize<EventKey> {
	private FastEncodingRule enc = new FastEncodingRule();

	@Override
	public byte[] serialize(EventKey key) {
		String[] array = { key.getTopic(), key.getKey(), key.getHost() };
		return enc.encode(array).array();
	}

	@Override
	public EventKey deserialize(byte[] in) {
		Object[] array = EncodingRule.decodeArray(ByteBuffer.wrap(in));
		return new EventKey((String) array[0], (String) array[1], (String) array[2]);
	}

	@Override
	public int size() {
		return -1;
	}

}
