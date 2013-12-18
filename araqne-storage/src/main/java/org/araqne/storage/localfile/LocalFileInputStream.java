package org.araqne.storage.localfile;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageInputStream;

public class LocalFileInputStream extends StorageInputStream {
	private LocalFilePath path;
	private final RandomAccessFile source;
	
	LocalFileInputStream(LocalFilePath path) throws IOException {
		this.path = path;
		this.source = new RandomAccessFile(path.getFile(), "r");
	}
	
	@Deprecated
	public LocalFileInputStream(RandomAccessFile r) throws IOException {
		this.source = r;
	}

	@Override
	public void readFully(ByteBuffer buf, long startPos) throws IOException {
		synchronized (source) {
			source.seek(startPos);
			int r = source.getChannel().read(buf);
			if (r != buf.capacity())
				throw new IOException("data underflow");
			buf.limit(r);
			buf.position(0);
		}
	}

	@Override
	public void close() throws IOException {
		source.close();
	}

	@Override
	public void readFully(byte[] b) throws IOException {
		readFully(b, 0, b.length);
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		source.readFully(b, off, len);
	}

	@Override
	public int skipBytes(int n) throws IOException {
		return source.skipBytes(n);
	}

	@Override
	public boolean readBoolean() throws IOException {
		return source.readBoolean();
	}

	@Override
	public byte readByte() throws IOException {
		return source.readByte();
	}

	@Override
	public int readUnsignedByte() throws IOException {
		return source.readUnsignedByte();
	}

	@Override
	public short readShort() throws IOException {
		return source.readShort();
	}

	@Override
	public int readUnsignedShort() throws IOException {
		return source.readUnsignedShort();
	}

	@Override
	public char readChar() throws IOException {
		return source.readChar();
	}

	@Override
	public int readInt() throws IOException {
		return source.readInt();
	}

	@Override
	public long readLong() throws IOException {
		return source.readLong();
	}

	@Override
	public float readFloat() throws IOException {
		return source.readFloat();
	}

	@Override
	public double readDouble() throws IOException {
		return source.readDouble();
	}

	@Override
	public String readLine() throws IOException {
		return source.readLine();
	}

	@Override
	public String readUTF() throws IOException {
		return source.readUTF();
	}

	@Override
	public long length() throws IOException {
		return source.length();
	}

	@Override
	public void seek(long pos) throws IOException {
		source.seek(pos);
	}

	@Override
	public int read(byte[] b) throws IOException {
		return source.read(b);
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return source.read(b, off, len);
	}

	@Override
	public FilePath getPath() {
		return path;
	}

	@Override
	public int read() throws IOException {
		return source.read();
	}

	@Override
	public long getPos() throws IOException {
		return source.getFilePointer();
	}

}
