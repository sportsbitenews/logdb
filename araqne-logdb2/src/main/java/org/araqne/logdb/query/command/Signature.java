/*
 * Copyright 2013 Eediom Inc.
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

import org.araqne.logdb.Row;
import org.araqne.logdb.QueryCommand;

public class Signature extends QueryCommand {

	@Override
	public void onPush(Row m) {
		String line = (String) m.get("line");
		if (line == null)
			return;

		String sig = makeSignature(line);
		m.put("signature", sig);
		pushPipe(m);
	}

	@Override
	public boolean isReducer() {
		return false;
	}

	private static String makeSignature(String line) {
		StringBuilder sb = new StringBuilder(line.length());
		boolean inQuote = false;
		for (int i = 0; i < line.length(); ++i) {
			char c = line.charAt(i);
			if (c == '\"')
				inQuote = !inQuote;
			if (Character.isLetterOrDigit(c))
				continue;
			if (!inQuote)
				sb.append(c);
		}
		return sb.toString();
	}

	@Override
	public String toString() {
		return "signature";
	}
	
}
