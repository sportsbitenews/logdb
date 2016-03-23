package org.araqne.logdb.cep.offheap.engine.serialize;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

public interface Serialize<T> {
	public byte[] serialize(T value);

	public T deserialize(byte[] in);

	// -1 if variable size
	public int size();

	Serialize<String> STRING = new Serialize<String>() {

		private String charsetName = "UTF-8";

		@Override
		public byte[] serialize(String value) {
			if (Charset.isSupported(charsetName))
				try {
					return value.getBytes(charsetName);
				} catch (UnsupportedEncodingException e) {
					throw new IllegalStateException(e);
				}
			else
				return value.getBytes(Charset.defaultCharset());
		}

		@Override
		public String deserialize(byte[] in) {// , int offset, int length) {
			if (Charset.isSupported(charsetName))
				return new String(in, Charset.forName(charsetName));
			else
				return new String(in, Charset.defaultCharset());
		}

		@Override
		public int size() {
			return -1; // variable size
		}

	};
}
