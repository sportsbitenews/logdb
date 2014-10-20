package org.araqne.logdb.geoip;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.geoip.GeoMacService;
import org.araqne.geoip.NicVendor;
import org.araqne.logdb.LookupHandler;
import org.araqne.logdb.LookupHandlerRegistry;

@Component(name = "logdb-geomac")
public class GeoMacLookupHandler implements LookupHandler {

	@Requires
	private GeoMacService macLookup;

	@Requires
	private LookupHandlerRegistry lookup;

	@Validate
	public void start() {
		lookup.addLookupHandler("geomac", this);
	}

	@Invalidate
	public void stop() {
		if (lookup != null)
			lookup.removeLookupHandler("geomac");
	}

	@Override
	public Object lookup(String srcField, String dstField, Object value) {
		if (value == null)
			return null;

		String mac = value.toString().toLowerCase();
		NicVendor vendor = macLookup.findByMac(mac);
		if (vendor == null)
			return null;

		if (dstField.equals("country"))
			return vendor.getCountry();
		else if (dstField.equals("name"))
			return vendor.getName();

		return null;
	}
}
