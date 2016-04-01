/*
 * Copyright 2009 NCHOVY
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
package org.araqne.rss;

public class RssCategory {
	private int id;
	private RssFeed rssFeed;
	private String name;
	private int isEnabled;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public RssFeed getRssFeed() {
		return rssFeed;
	}

	public void setRssFeed(RssFeed rssFeed) {
		this.rssFeed = rssFeed;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getIsEnabled() {
		return isEnabled;
	}

	public void setIsEnabled(int isEnabled) {
		this.isEnabled = isEnabled;
	}
}