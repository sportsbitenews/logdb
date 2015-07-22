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
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.api.InetAddresses;
import org.araqne.geoip.GeoIpLocation;
import org.araqne.geoip.GeoIpService;
import org.araqne.logdb.LookupHandler2;
import org.araqne.logdb.LookupHandlerRegistry;
import org.araqne.logdb.LookupTable;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;

@Component(name = "logdb-geoip")
public class GeoipLookupExtender implements LookupHandler2 {
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
	public LookupTable newTable(String keyField, Map<String, String> outputFields) {
		return new GeoIpLookupTable(keyField, outputFields);
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

	private class GeoIpLookupTable implements LookupTable {

		private String keyField;
		private Map<String, String> outputFields;
		private boolean countryOnly;
		private String countryOutputField;

		public GeoIpLookupTable(String keyField, Map<String, String> outputFields) {
			this.keyField = keyField;
			this.outputFields = outputFields;
			this.countryOnly = outputFields.containsKey("country") && outputFields.size() == 1;
			this.countryOutputField = outputFields.get("country");
		}

		@Override
		public void lookup(Row row) {
			locate(row);
		}

		@Override
		public void lookup(RowBatch rowBatch) {
			if (rowBatch.selectedInUse) {
				for (int i = 0; i < rowBatch.size; i++) {
					int p = rowBatch.selected[i];
					Row row = rowBatch.rows[p];
					locate(row);
				}
			} else {
				for (int i = 0; i < rowBatch.size; i++) {
					Row row = rowBatch.rows[i];
					locate(row);
				}
			}
		}

		private void locate(Row row) {
			Object key = row.get(keyField);
			InetAddress ip = null;
			if (key instanceof InetAddress)
				ip = (InetAddress) key;

			if (key instanceof String) {
				try {
					ip = InetAddresses.forString((String) key);
				} catch (Throwable t) {
				}
			}

			if (ip == null)
				return;

			if (countryOnly) {
				String country = geoip.locateCountry(ip);
				row.put(countryOutputField, country);
				return;
			}

			GeoIpLocation location = geoip.locate(ip);
			if (location == null)
				return;

			for (String outputField : outputFields.keySet()) {
				String renameField = outputFields.get(outputField);

				if (outputField.equals("country"))
					row.put(renameField, location.getCountry());
				else if (outputField.equals("region"))
					row.put(renameField, location.getRegion());
				else if (outputField.equals("city"))
					row.put(renameField, location.getCity());
				else if (outputField.equals("latitude"))
					row.put(renameField, location.getLatitude());
				else if (outputField.equals("longitude"))
					row.put(renameField, location.getLongitude());
			}
		}
	}
}
