/**
 * Copyright 2014 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb.query.command;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.Strings;
import org.araqne.logdb.TimeSpan;
import org.araqne.logdb.query.expr.Expression;
import org.slf4j.LoggerFactory;

/**
 * @since 2.4.24
 * @author xeraph
 * 
 */
public class Exec extends QueryCommand {
	private final org.slf4j.Logger slog = LoggerFactory.getLogger(Exec.class);
	private String command;
	private List<Expression> args;
	private final int argCount;
	private final TimeSpan timeout;

	public Exec(String command, List<Expression> args, TimeSpan timeout) {
		this.command = command;
		this.args = args;
		this.argCount = args.size();
		this.timeout = timeout;
	}

	public String getCommand() {
		return command;
	}

	public List<Expression> getArguments() {
		return args;
	}

	@Override
	public String getName() {
		return "exec";
	}

	@Override
	public void onPush(Row row) {
		runCommand(row);
		pushPipe(row);
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				int p = rowBatch.selected[i];
				Row row = rowBatch.rows[p];
				runCommand(row);
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];
				runCommand(row);
			}
		}

		pushPipe(rowBatch);
	}

	private void runCommand(Row row) {
		try {
			String[] cmdArray = new String[argCount + 1];
			cmdArray[0] = command;
			for (int i = 1; i <= argCount; i++) {
				Object value = args.get(i - 1).eval(row);
				cmdArray[i] = value == null ? "" : value.toString();
			}

			if (slog.isDebugEnabled())
				slog.debug("araqne logdb: exec command [{}]", Arrays.asList(cmdArray));

			Process p = Runtime.getRuntime().exec(cmdArray);
			p.getInputStream().close();
			p.getOutputStream().close();
			p.getErrorStream().close();

			if (timeout != null) {
				long expire = System.nanoTime() + timeout.getMillis() * 1000000;
				while (true) {
					try {
						p.exitValue();
						break;
					} catch (IllegalThreadStateException e) {
						if (expire <= System.nanoTime()) {
							p.destroy();
							break;
						}

						// not yet terminated
						Thread.sleep(10);
					}
				}
			} else {
				p.waitFor();
			}

		} catch (IOException e) {
			slog.error("araqne logdb: exec failed: " + command, e);
		} catch (InterruptedException e) {
		}
	}

	@Override
	public String toString() {
		String timeoutOpt = "";
		if (timeout != null)
			timeoutOpt = " timeout=" + timeout;

		return "exec" + timeoutOpt + " " + command + " " + Strings.join(args, ", ");
	}
}
