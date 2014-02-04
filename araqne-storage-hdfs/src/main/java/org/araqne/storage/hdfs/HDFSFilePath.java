package org.araqne.storage.hdfs;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.FilePathNameFilter;
import org.araqne.storage.api.StorageInputStream;
import org.araqne.storage.api.StorageOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

public class HDFSFilePath implements FilePath {
	static final String PROTOCOL_NAME = "hdfs";
	static final String PROTOCOL_STRING = "hdfs://";
	private final HDFSCluster root;
	private final Path path;
	
	public HDFSFilePath(HDFSCluster root, String path) {
		this(root, new Path(path));
	}
	
	public HDFSFilePath(HDFSCluster root, Path path) {
		this.root = root;
		this.path = path;
	}
	
	HDFSCluster getRoot() {
		return root;
	}

	@Override
	public int compareTo(FilePath o) {
		if (o instanceof HDFSFilePath) {
			HDFSFilePath rhs = (HDFSFilePath) o;
			int r = root.getAlias().compareTo(rhs.root.getAlias());
			if (r != 0)
				return r;
			else 
				return path.compareTo(rhs.path);
		}
		return getProtocol().compareTo(o.getProtocol());
	}

	@Override
	public String getProtocol() {
		return PROTOCOL_NAME;
	}

	@Override
	public String getAbsolutePath() throws SecurityException {
		return root.toString() + path.toUri().toString();
	}

	@Override
	public String getCanonicalPath() throws IOException, SecurityException {
		return getAbsolutePath();
	}

	@Override
	public String getName() {
		return path.getName();
	}

	@Override
	public boolean exists() throws SecurityException {
		try {
			return root.getFileSystem().exists(path);
		} catch (IOException e) {
			throw new IllegalStateException("Unexpected IOException", e);
		}
	}

	@Override
	public boolean mkdirs() throws SecurityException {
		try {
			return root.getFileSystem().mkdirs(path);
		} catch (IOException e) {
		}
		
		return false;
	}

	@Override
	public boolean delete() throws SecurityException {
		try {
			return root.getFileSystem().delete(path, true);
		} catch (IOException e) {
		}
		
		return false;
	}

	@Override
	public boolean renameTo(FilePath dest) throws SecurityException {
		if (!(dest instanceof HDFSFilePath))
			return false;
		
		HDFSFilePath destPath = (HDFSFilePath) dest;
		if (!root.equals(destPath.root))
			return false;
		
		try {
			return root.getFileSystem().rename(path, destPath.path);
		} catch (IOException e) {
			throw new IllegalStateException("Unexpected IOException", e);
		}
	}

	@Override
	public boolean isDirectory() throws SecurityException {
		try {
			return root.getFileSystem().isDirectory(path);
		} catch (IOException e) {
			throw new IllegalStateException("Unexpected IOException", e);
		}
	}

	@Override
	public boolean isFile() throws SecurityException {
		try {
			return root.getFileSystem().isFile(path);
		} catch (IOException e) {
			throw new IllegalStateException("Unexpected IOException", e);
		}
	}

	@Override
	public boolean canRead() throws SecurityException {
		String username = System.getProperty("user.name");
		FileStatus fs;
		try {
			fs = root.getFileSystem().getFileStatus(path);
		} catch (IOException e) {
			throw new IllegalStateException("Unexpected IOException", e);
		}
		FsPermission permission = fs.getPermission();
		// TODO handle user group
		FsAction action = (username.equals(fs.getOwner()))? permission.getUserAction(): permission.getOtherAction(); 
		return action.and(FsAction.READ).equals(FsAction.READ);
	}

	@Override
	public boolean canWrite() throws SecurityException {
		String username = System.getProperty("user.name");
		FileStatus fs;
		try {
			fs = root.getFileSystem().getFileStatus(path);
		} catch (IOException e) {
			throw new IllegalStateException("Unexpected IOException", e);
		}
		FsPermission permission = fs.getPermission();
		// TODO handle user group
		FsAction action = (username.equals(fs.getOwner()))? permission.getUserAction(): permission.getOtherAction(); 
		return action.and(FsAction.WRITE).equals(FsAction.WRITE);
	}

	@Override
	public char getSeperatorChar() {
		return Path.SEPARATOR_CHAR;
	}

	@Override
	public long length() throws SecurityException {
		try {
			return root.getFileSystem().getFileStatus(path).getLen();
		} catch (IOException e) {
			throw new IllegalStateException("Unexpected IOException", e);
		}
	}

	@Override
	public FilePath[] listFiles() throws SecurityException {
		List<FilePath> ret = new ArrayList<FilePath>();
		
		try {
			RemoteIterator<LocatedFileStatus> it = root.getFileSystem().listFiles(path, false);
			while (it.hasNext()) {
				ret.add(new HDFSFilePath(root, it.next().getPath()));
			}
		} catch (IOException e) {
			throw new IllegalStateException("Unexpected IOException", e);
		}

		return (FilePath[]) (ret.toArray(new FilePath[0]));
	}

	@Override
	public FilePath[] listFiles(FilePathNameFilter filter) throws SecurityException {
		List<FilePath> ret = new ArrayList<FilePath>();
		
		try {
		RemoteIterator<LocatedFileStatus> it = root.getFileSystem().listFiles(path, false);
		while (it.hasNext()) {
			FilePath curr = new HDFSFilePath(root, it.next().getPath());
			if (filter.accept(this, curr.getName()))
				ret.add(curr);
		}
		} catch (IOException e) {
			throw new IllegalStateException("Unexpected IOException", e);
		}

		return (FilePath[]) (ret.toArray(new FilePath[0]));
	}

	@Override
	public StorageInputStream newInputStream() throws IOException {
		return new HDFSFileInputStream(this, root.getFileSystem().open(path));
	}

	@Override
	public StorageOutputStream newOutputStream(boolean append) throws IOException {
		return new HDFSFileOutputStream(this, append? root.getFileSystem().append(path): root.getFileSystem().create(path));
	}

	@Override
	public FilePath newFilePath(String child) {
		return new HDFSFilePath(root, new Path(path, child));
	}

	private static final SecureRandom random = new SecureRandom();
	@Override
	public FilePath createTempFilePath(String prefix, String suffix) throws IOException, IllegalArgumentException, SecurityException {
		long n = random.nextLong();
		
		// handle corner case
		n = (n == Long.MIN_VALUE)? 0: Math.abs(n);
		String name = prefix + Long.toString(n) + suffix;
		return newFilePath(name);
	}

	@Override
	public FilePath getParentFilePath() throws SecurityException {
		Path parentPath = path.getParent();
		
		if (parentPath == null)
			return null;
		
		return new HDFSFilePath(root, parentPath);
	}

	@Override
	public FilePath getAbsoluteFilePath() throws SecurityException {
		return this;
	}

	@Override
	public long getFreeSpace() throws SecurityException {
		try {
			return root.getFileSystem().getStatus(path).getRemaining();
		} catch (IOException e) {
			throw new IllegalStateException("Unexpected IOException", e);
		}
	}

	@Override
	public long getUsableSpace() throws SecurityException {
		try {
			return root.getFileSystem().getStatus(path).getRemaining();
		} catch (IOException e) {
			throw new IllegalStateException("Unexpected IOException", e);
		}
	}

	@Override
	public long getTotalSpace() throws SecurityException {
		try {
			return root.getFileSystem().getStatus(path).getCapacity();
		} catch (IOException e) {
			throw new IllegalStateException("Unexpected IOException", e);
		}
	}
	
	@Override
	public String toString() {
		return root.toString() + path.toString();
	}

}
