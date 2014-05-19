/*
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
package org.araqne.logparser.syslog.ibm;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

/**
 * @author kyun
 */

public class ProventiaIdsLogParserTest {
	

	private static final String[] from = new String[] {
		"1.3.6.1.2.1.1.3.0", 
		"1.3.6.1.6.3.1.1.4.1.0", 
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.1.0",
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.2.0", 
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.3.0",
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.4.0", 
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.5.0", 
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.6.0", 
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.7.0",
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.8.0", 
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.9.0",  
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.10.0", 
		"1.3.6.1.4.1.2499.1.1.2.1.1.1.1.11.0"
	};
	
	@Test
	public void testParser() {
		
		Map<String, Object> raw = new HashMap<String, Object>();
		
		raw.put(from[0], "");
		raw.put(from[1], "");
		raw.put(from[2], "");
		raw.put(from[3], "");
		raw.put(from[4], "");
		raw.put(from[5], "");
		raw.put(from[6], "");
		raw.put(from[7], "");
		raw.put(from[8], "");
		raw.put(from[9], "");
		raw.put(from[10], "");
		raw.put(from[11], "");
		raw.put(from[12], "Host Name:; Protocol Name:TCP; target-ip-addr-start:220.103.229.115; "
				+ "target-ip-addr-end:220.103.229.115; :URL:/shop_client/scproxy.omp; :accessed:yes;"
				+ " :adapter:A (1A); :arg:REQ=402230313035363838393936310053534f4344503132303032000000000"
				+ "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
				+ "0000000000000000000000000000000000000000000000; :arg-length:196; :char:0; :code:200; "
				+ ":httpsvr:Apache; :pam.name.maxrepeatedchar:100; :server:220.103.229.115; "
				+ "event-info:URL=/shop_client/scproxy.omp,arg=REQ=402230313035363838393936310053534f4344503132"
				+ "3030320000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
				+ "000000000000000000000000000000000000000000000000000000000,arg-length=196,char=0,pam.name.maxrepeatedchar=100,"
				+ "server=220.103.229.115,httpsvr=Apache,accessed=yes,code=200,adapter=A (1A); event-type:Attack;");
		
		ProventiaIdsLogParser parser = new ProventiaIdsLogParser();
		
		Map<String, Object> m = parser.parse(raw);

		assertEquals("", m.get("sysUpTime"));
		assertEquals("", m.get("snmptrapOID"));
		assertEquals("", m.get("signature"));
		assertEquals("", m.get("time"));
		assertEquals("", m.get("protocol"));
		assertEquals("", m.get("srcip"));
		assertEquals("", m.get("dstip"));
		assertEquals("", m.get("ICMPType"));
		assertEquals("", m.get("ICMPCode"));
		assertEquals("", m.get("srcport"));
		assertEquals("", m.get("dstport"));
		assertEquals("", m.get("ActionList"));
		assertEquals("Host Name:; Protocol Name:TCP; target-ip-addr-start:220.103.229.115; "
				+ "target-ip-addr-end:220.103.229.115; :URL:/shop_client/scproxy.omp; :accessed:yes;"
				+ " :adapter:A (1A); :arg:REQ=402230313035363838393936310053534f4344503132303032000000000"
				+ "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
				+ "0000000000000000000000000000000000000000000000; :arg-length:196; :char:0; :code:200; "
				+ ":httpsvr:Apache; :pam.name.maxrepeatedchar:100; :server:220.103.229.115; "
				+ "event-info:URL=/shop_client/scproxy.omp,arg=REQ=402230313035363838393936310053534f4344503132"
				+ "3030320000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
				+ "000000000000000000000000000000000000000000000000000000000,arg-length=196,char=0,pam.name.maxrepeatedchar=100,"
				+ "server=220.103.229.115,httpsvr=Apache,accessed=yes,code=200,adapter=A (1A); event-type:Attack;", m.get("extra"));
		
		
		assertEquals("/shop_client/scproxy.omp", m.get("url"));
		assertEquals("220.103.229.115", m.get("server"));
		assertEquals("Attack", m.get("event_type"));
		assertEquals("TCP", m.get("proto_name"));

	
	
	}
	

	
	
	

}
