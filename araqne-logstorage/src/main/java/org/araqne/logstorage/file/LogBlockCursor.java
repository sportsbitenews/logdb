package org.araqne.logstorage.file;

import java.io.Closeable;
import java.util.Iterator;

public interface LogBlockCursor extends Iterator<LogBlock>, Closeable {
}
