package org.araqne.storage.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageOutputStream;

public class HDFSFileOutputStream extends StorageOutputStream {
	private final HDFSFilePath path;
	private final FSDataOutputStream stream;
	
	public HDFSFileOutputStream(HDFSFilePath path, FSDataOutputStream stream) {
		this.path = path;
		this.stream = stream;
	}

	@Override
	public FilePath getPath() {
		return path;
	}

	@Override
	public void sync() throws IOException {
		stream.hsync();
	}

	@Override
	public long getPos() throws IOException {
		return stream.getPos();
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		stream.writeBoolean(v);
	}

	@Override
	public void writeByte(int v) throws IOException {
		stream.writeByte(v);
	}

	@Override
	public void writeShort(int v) throws IOException {
		stream.writeShort(v);
	}

	@Override
	public void writeChar(int v) throws IOException {
		stream.writeChar(v);
	}

	@Override
	public void writeInt(int v) throws IOException {
		stream.writeInt(v);
	}

	@Override
	public void writeLong(long v) throws IOException {
		stream.writeLong(v);
	}

	@Override
	public void writeFloat(float v) throws IOException {
		stream.writeFloat(v);
	}

	@Override
	public void writeDouble(double v) throws IOException {
		stream.writeDouble(v);
	}

	@Override
	public void writeBytes(String s) throws IOException {
		stream.writeBytes(s);
		
	}

	@Override
	public void writeChars(String s) throws IOException {
		stream.writeChars(s);
	}

	@Override
	public void writeUTF(String s) throws IOException {
		stream.writeUTF(s);
	}

	@Override
	public void write(int b) throws IOException {
		stream.write(b);
	}
	
	@Override
	public void close() throws IOException {
		stream.close();
	}

}
