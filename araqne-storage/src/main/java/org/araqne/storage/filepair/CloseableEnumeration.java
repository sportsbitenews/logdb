package org.araqne.storage.filepair;

import java.io.Closeable;
import java.util.Enumeration;

public interface CloseableEnumeration<T> extends Enumeration<T>, Closeable {

}
