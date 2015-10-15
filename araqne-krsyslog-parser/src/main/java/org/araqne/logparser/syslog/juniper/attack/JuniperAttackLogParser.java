/*
 * Copyright 2010 NCHOVY
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
package org.araqne.logparser.syslog.juniper.attack;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.araqne.logparser.syslog.internal.PatternFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JuniperAttackLogParser {
	private final Logger logger = LoggerFactory.getLogger(JuniperAttackLogParser.class.getName());
	private PatternFinder<JuniperAttackLogPattern> patternMap;

	public JuniperAttackLogParser() throws IOException {
		patternMap = loadPatterns();
	}

	public static JuniperAttackLogParser newInstance() {
		try {
			return new JuniperAttackLogParser();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	private static PatternFinder<JuniperAttackLogPattern> loadPatterns() throws IOException {
		PatternFinder<JuniperAttackLogPattern> m = PatternFinder.newInstance();

		load(m,
				"Emergency (00005)",
				"SYN flood! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto TCP (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Emergency (00006)",
				"Teardrop attack! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto { TCP | UDP | <protocol> } (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m, "Emergency (00007)",
				"Ping of Death! From <src-ip> to <dst-ip>, proto 1 (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Alert (00004)",
				"WinNuke attack! From <src-ip>:<src-port> to <dst-ip>:139, proto TCP (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Alert (00008)",
				"IP spoofing! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto { TCP | UDP | <protocol> } (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Alert (00009)",
				"Source Route IP option! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto { TCP | UDP | <protocol> } (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Alert (00010)",
				"Land attack! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto TCP (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m, "Alert (00011)",
				"ICMP flood! From <src-ip> to <dst-ip>, proto 1 (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Alert (00012)",
				"UDP flood! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto UDP (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Alert (00016)",
				"Port scan! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto { TCP | UDP | <protocol> } (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m, "Alert (00017)",
				"Address sweep! From <src-ip> to <dst-ip>, proto 1 (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Critical (00032)",
				"Malicious URL! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto TCP (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Critical (00033)",
				"Src IP session limit! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto { TCP | UDP | <protocol> } (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Critical (00412)",
				"SYN fragment! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto TCP (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Critical (00413)",
				"No TCP flag! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto { TCP | UDP | <protocol> } (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Critical (00414)",
				"Unknown protocol! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto <protocol> (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Critical (00415)",
				"Bad IP option! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto { TCP | UDP | <protocol> } (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Critical (00430)",
				"Dst IP session limit! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto { TCP | UDP | <protocol> } (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Critical (00431)",
				"ZIP file blocked! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto { TCP | UDP | <protocol> } (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Critical (00432)",
				"Java applet blocked! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto { TCP | UDP | <protocol> } (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Critical (00433)",
				"EXE file blocked! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto { TCP | UDP | <protocol> } (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Critical (00434)",
				"ActiveX control blocked! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto { TCP | UDP | <protocol> } (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m, "Critical (00435)",
				"ICMP fragment! From <src-ip> to <dst-ip>, proto 1 (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m, "Critical (00436)",
				"Large ICMP packet! From <src-ip> to <dst-ip>, proto 1 (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Critical (00437)",
				"SYN and FIN bits! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto TCP (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Critical (00438)",
				"FIN but no ACK bit! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto TCP (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Critical (00439)",
				"SYN-ACK-ACK Proxy DoS! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto TCP (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		load(m,
				"Critical (00440)",
				"Fragmented traffic! From <src-ip>:<src-port> to <dst-ip>:<dst-port>, proto { TCP | UDP | <protocol> } (zone <zone-name>, int <interface-name>). Occurred <none> times.");
		return m;
	}

	private static void load(PatternFinder<JuniperAttackLogPattern> m, String category, String patternString) {
		JuniperAttackLogPattern pattern = JuniperAttackLogPattern.from(category, patternString);
		m.register(pattern.getConstElements().get(0), pattern);
	}

	public Map<String, Object> parse(String line) {
		Set<JuniperAttackLogPattern> patterns = patternMap.find(line);
		for (JuniperAttackLogPattern pattern : patterns) {
			Map<String, Object> result = null;
			try {
				result = pattern.parse(line);
				if (result != null)
					return result;
			} catch (Throwable t) {
				logger.warn("araqne syslog parser: cannot parse juniper attack log", t);
			}
		}

		return null;
	}

	public Set<String> getPatternKeySet() {
		return patternMap.fingetPrints();
	}
}
