package org.araqne.logdb.hprof;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.araqne.logdb.DriverQueryCommand;

import edu.tufts.eaftan.hprofparser.parser.HprofParser;

public class JmapCommand extends DriverQueryCommand {

	private String path;

	public JmapCommand(String path) {
		this.path = path;
	}

	@Override
	public String getName() {
		return "jmap";
	}

	@Override
	public void run() {
		HprofParser parser = new HprofParser(new JmapRecordHandler(this));

		FileInputStream fs = null;
		DataInputStream in = null;
		try {
			fs = new FileInputStream(path);
			in = new DataInputStream(new BufferedInputStream(fs));

			parser.parse(in);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (Throwable t) {
				}
			}

			if (fs != null) {
				try {
					fs.close();
				} catch (Throwable t) {
				}
			}
		}
	}

	@Override
	public String toString() {
		return "jmap " + path;
	}
}