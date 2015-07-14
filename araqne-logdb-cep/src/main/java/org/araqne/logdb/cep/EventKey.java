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
package org.araqne.logdb.cep;



public class EventKey {
	private final String topic;
	private final String key;

	// optional host value for log tick
	private String host;

	private final int hashCode;

	public EventKey(String topic, String key) {
		this.topic = topic;
		this.key = key;
		this.hashCode = topic.hashCode() ^ key.hashCode();
	}

	public String getTopic() {
		return topic;
	}

	public String getKey() {
		return key;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	@Override
	public int hashCode() {
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;

		// must be same class
		if (getClass() != obj.getClass())
			return false;

		// topic and key is always not null
		EventKey other = (EventKey) obj;
		return key.equals(other.key) && topic.equals(other.topic);
	}

	@Override
	public String toString() {
		if (host == null)
			return "topic=" + topic + ", key=" + key;
		return "topic=" + topic + ", key=" + key + ", host=" + host;
	}

	public static byte[] marshal(EventKey key) {
		StringBuffer sb = new StringBuffer();
		sb.append(key.getTopic());
		sb.append(":");
		sb.append(key.getKey());
		sb.append(":");
		if(key.getHost() != null)
			sb.append(key.getHost());
		sb.append(":");

		return sb.toString().getBytes();
	}

	public static EventKey parse(byte[] bs) {

		try {
			String keys = new String(bs);

			String[] fields = new String[3];

			int e = -1;
			int s = 0;

			for(int i = 0; i < fields.length ; i ++) {
				s = e +1;
				e = keys.indexOf(":", s);
				fields[i]  = keys.substring(s, e);
			}

			EventKey evt = new EventKey(fields[0], fields[1]);
			if(fields[2] != null && !fields[2].trim().isEmpty())
				evt.setHost(fields[2]);

			return evt;
		} catch (Exception e) {
			return null;
		}

	}

}
