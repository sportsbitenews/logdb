package org.araqne.logdb;

import java.io.IOException;

public interface FileMover {

	void move(String from, String to) throws IOException;

}
