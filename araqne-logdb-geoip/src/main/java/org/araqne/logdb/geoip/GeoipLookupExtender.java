/*
 * Copyright 2011 Future Systems
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
package org.araqne.logdb.geoip;

import java.net.InetAddress;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.api.InetAddresses;
import org.araqne.geoip.GeoIpLocation;
import org.araqne.geoip.GeoIpService;
import org.araqne.logdb.LookupHandler;
import org.araqne.logdb.LookupHandlerRegistry;

@Component(name = "logdb-geoip")
public class GeoipLookupExtender implements LookupHandler {
	@Requires
	private GeoIpService geoip;

	@Requires
	private LookupHandlerRegistry lookup;

	@Validate
	public void start() {
		lookup.addLookupHandler("geoip", this);
	}

	@Invalidate
	public void stop() {
		if (lookup != null)
			lookup.removeLookupHandler("geoip");
	}

	@Override
	public Object lookup(String srcField, String dstField, Object value) {
		InetAddress ip = null;
		if (value instanceof InetAddress)
			ip = (InetAddress) value;

		if (value instanceof String) {
			try {
				ip = InetAddresses.forString((String) value);
			} catch (Throwable t) {
				return null;
			}
		}

		if (ip == null)
			return null;

		if (dstField.equals("country"))
			return geoip.locateCountry(ip);

		GeoIpLocation location = geoip.locate(ip);
		if (location == null)
			return null;

		if (dstField.equals("region"))
			return location.getRegion();
		else if (dstField.equals("city"))
			return location.getCity();
		else if (dstField.equals("latitude"))
			return location.getLatitude();
		else if (dstField.equals("longitude"))
			return location.getLongitude();

		return null;
	}
}
