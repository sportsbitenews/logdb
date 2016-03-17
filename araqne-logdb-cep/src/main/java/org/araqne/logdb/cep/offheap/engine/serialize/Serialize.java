package org.araqne.logdb.cep.offheap.engine.serialize;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public interface Serialize<T> {

	// public byte[] serialize(T value);
	public ByteBuffer serialize(T value);

	// public T deserialize(byte[] in, int offset, int length);
	public T deserialize(ByteBuffer bb);

	// -1 for variable size
	public int size();

	Serialize<String> STRING = new Serialize<String>() {

		private String charsetName = "UTF-8";

//		@Override
//		public byte[] serialize(String value) {
//			if (Charset.isSupported(charsetName))
//				try {
//					return value.getBytes(charsetName);
//				} catch (UnsupportedEncodingException e) {
//					throw new IllegalStateException(e);
//				}
//			else
//				return value.getBytes(Charset.defaultCharset());
//		}

		@Override
		public ByteBuffer serialize(String value) {
			if (Charset.isSupported(charsetName))
				try {
					return ByteBuffer.wrap(value.getBytes(charsetName));
				} catch (UnsupportedEncodingException e) {
					throw new IllegalStateException(e);
				}
			else
				return ByteBuffer.wrap(value.getBytes(Charset.defaultCharset()));
		}
		
//		@Override
//		public String deserialize(byte[] in, int offset, int length) {
//			if (Charset.isSupported(charsetName))
//				return new String(in, offset, length, Charset.forName(charsetName));
//			else
//				return new String(in, offset, length, Charset.defaultCharset());
//		}
//		
		@Override
		public String deserialize(ByteBuffer bb) {
			if (Charset.isSupported(charsetName))
				return new String(bb.array(), Charset.forName(charsetName));
			else
				return new String(bb.array(), Charset.defaultCharset());
		}

		@Override
		public int size() {
			return -1; // variable size
		}

	};

	// Serializer<Integer> INTEGER = new Serializer<Integer>() {
	// @Override
	// public void serialize(byte[] out, Integer value) throws IOException {
	// out.writeInt(value);
	// }
	//
	// @Override
	// public Integer deserialize(byte[] in, int available) throws IOException {
	// return in.readInt();
	// }
	//
	// @Override
	// public int fixedSize() {
	// return 4;
	// }
	//
	// };
	//
	// Serializer<Boolean> BOOLEAN = new Serializer<Boolean>() {
	// @Override
	// public void serialize(byte[] out, Boolean value) throws IOException {
	// out.writeBoolean(value);
	// }
	//
	// @Override
	// public Boolean deserialize(byte[] in, int available) throws IOException {
	// if (available == 0)
	// return null;
	// return in.readBoolean();
	// }
	//
	// @Override
	// public int fixedSize() {
	// return 1;
	// }
	//
	// };

}
