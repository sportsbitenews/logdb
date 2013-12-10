package org.araqne.storage.localfile;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageOutputStream;

public class LocalFileOutputStream extends StorageOutputStream {
	private LocalFilePath path;
	private final RandomAccessFile target;
	
	public LocalFileOutputStream(LocalFilePath path, boolean append) throws IOException {
		this.path = path;
		this.target = new RandomAccessFile(path.getFile(), "rw");
		if (append) {
			target.seek(target.length());
		}
	}

	@Override
	public void close() throws IOException {
		target.close();
	}

	@Override
	public void write(int b) throws IOException {
		target.write(b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		target.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		target.write(b, off, len);
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		target.writeBoolean(v);
	}

	@Override
	public void writeByte(int v) throws IOException {
		target.writeByte(v);
	}

	@Override
	public void writeShort(int v) throws IOException {
		target.writeShort(v);
	}

	@Override
	public void writeChar(int v) throws IOException {
		target.writeChar(v);
	}

	@Override
	public void writeInt(int v) throws IOException {
		target.writeInt(v);
	}

	@Override
	public void writeLong(long v) throws IOException {
		target.writeLong(v);
	}

	@Override
	public void writeFloat(float v) throws IOException {
		target.writeFloat(v);
	}

	@Override
	public void writeDouble(double v) throws IOException {
		target.writeDouble(v);
	}

	@Override
	public void writeBytes(String s) throws IOException {
		target.writeBytes(s);
	}

	@Override
	public void writeChars(String s) throws IOException {
		target.writeChars(s);
	}

	@Override
	public void writeUTF(String str) throws IOException {
		target.writeUTF(str);
	}

	@Override
	public FilePath getPath() {
		return path;
	}

	@Override
	public void sync() throws IOException {
		target.getFD().sync();
	}

}
