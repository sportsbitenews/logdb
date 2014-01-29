package org.araqne.storage.hdfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageInputStream;

public class HDFSFileInputStream extends StorageInputStream {
	private final HDFSFilePath path;
	private final FSDataInputStream stream;
	
	public HDFSFileInputStream(HDFSFilePath path, FSDataInputStream stream) {
		this.path = path;
		this.stream = stream;
	}

	@Override
	public FilePath getPath() {
		return path;
	}

	@Override
	public long length() throws IOException {
		return path.length();
	}

	@Override
	public void seek(long pos) throws IOException {
		stream.seek(pos);
	}

	@Override
	public long getPos() throws IOException {
		return stream.getPos();
	}

	@Override
	public int readBestEffort(ByteBuffer buf) throws IOException {
		int total = 0;
		while (buf.remaining() > 0) {
			int readBytes = stream.read(buf);
			if (readBytes < 0)
				break;
			total += readBytes;
		}
		return total;
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		stream.readFully(b, off, len);
	}
	
	@Override
	public long skip(long n) throws IOException {
		return stream.skip(n);
	}

	@Override
	public int skipBytes(int n) throws IOException {
		return stream.skipBytes(n);
	}

	@Override
	public boolean readBoolean() throws IOException {
		return stream.readBoolean();
	}

	@Override
	public byte readByte() throws IOException {
		return stream.readByte();
	}

	@Override
	public int readUnsignedByte() throws IOException {
		return stream.readUnsignedByte();
	}

	@Override
	public short readShort() throws IOException {
		return stream.readShort();
	}

	@Override
	public int readUnsignedShort() throws IOException {
		return stream.readUnsignedShort();
	}

	@Override
	public char readChar() throws IOException {
		return stream.readChar();
	}

	@Override
	public int readInt() throws IOException {
		return stream.readInt();
	}

	@Override
	public long readLong() throws IOException {
		return stream.readLong();
	}

	@Override
	public float readFloat() throws IOException {
		return stream.readFloat();
	}

	@Override
	public double readDouble() throws IOException {
		return stream.readFloat();
	}

	@SuppressWarnings("deprecation")
	@Override
	public String readLine() throws IOException {
		return stream.readLine();
	}

	@Override
	public String readUTF() throws IOException {
		return stream.readUTF();
	}

	@Override
	public int read() throws IOException {
		return stream.read();
	}
	
	@Override
	public int read(byte[] b) throws IOException {
		return stream.read(b);
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return stream.read(b, off, len);
	}
	
	@Override
	public int available() throws IOException {
		return stream.available();
	}

}
