package org.araqne.storage.filepair;

import java.io.IOException;
import java.io.OutputStream;

public abstract class RawDataBlock<T> {
	public abstract void serialize(OutputStream os) throws IOException;

}
