/*
 * Copyright 2014 Eediom Inc
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
package org.araqne.logparser.syslog.sourcefire;

import java.util.HashMap;
import java.util.Map;

import org.araqne.log.api.V1LogParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirePowerLogParser extends V1LogParser {
	private Logger slog = LoggerFactory.getLogger(this.getClass());

	@Override
	public Map<String, Object> parse(Map<String, Object> params) {
		Map<String, Object> m = new HashMap<String, Object>();
		
		try{
			m.put("SourceIp",params.get("1.3.6.1.4.1.14223.1.1.13"));
			m.put("DestinationIp",params.get("1.3.6.1.4.1.14223.1.1.14"));
			m.put("IpAddress",params.get("1.3.6.1.4.1.14223.1.1.15"));
			m.put("OsVendor",params.get("1.3.6.1.4.1.14223.1.1.18"));
			m.put("OsName",params.get("1.3.6.1.4.1.14223.1.1.19"));
			m.put("SignatureGenerator",params.get("1.3.6.1.4.1.14223.1.1.2"));
			m.put("OsVersion",params.get("1.3.6.1.4.1.14223.1.1.20"));
			m.put("SensorId",params.get("1.3.6.1.4.1.14223.1.1.29"));
			m.put("SignatureId",params.get("1.3.6.1.4.1.14223.1.1.3"));
			m.put("Impact",params.get("1.3.6.1.4.1.14223.1.1.32"));
			m.put("Version",params.get("1.3.6.1.4.1.14223.1.1.36"));
			m.put("ClientApplicationId",params.get("1.3.6.1.4.1.14223.1.1.37"));
			m.put("SignatureRevision",params.get("1.3.6.1.4.1.14223.1.1.4"));
			m.put("ImpactString",params.get("1.3.6.1.4.1.14223.1.1.43"));
			m.put("ClientApplicationTypeId",params.get("1.3.6.1.4.1.14223.1.1.44"));
			m.put("IpProtocol",params.get("1.3.6.1.4.1.14223.1.1.5"));
			m.put("EventMessage",params.get("1.3.6.1.4.1.14223.1.1.6"));
			m.put("SourcePort",params.get("1.3.6.1.4.1.14223.1.1.7"));
			m.put("DestinationPort",params.get("1.3.6.1.4.1.14223.1.1.8"));
			m.put("IOCCategory",params.get("1.3.6.1.4.1.14223.1.1.86"));
			m.put("IOCEvent",params.get("1.3.6.1.4.1.14223.1.1.87"));

			return m;

		} catch (Throwable t) {
			if (slog.isDebugEnabled())
				slog.debug("araqne syslog parser: sourcefile next-generation ips snmp parse error ", t);
			return params;
		}
	}
}
