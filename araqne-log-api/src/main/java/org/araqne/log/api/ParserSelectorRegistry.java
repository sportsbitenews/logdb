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
package org.araqne.log.api;

import java.util.List;

/**
 * @since 3.4.7
 */
public interface ParserSelectorRegistry {
	List<ParserSelectorProfile> getProfiles();

	ParserSelectorProfile getProfile(String name);

	void createProfile(ParserSelectorProfile profile);

	void removeProfile(String name);

	List<ParserSelectorProvider> getProviders();

	ParserSelectorProvider getProvider(String name);

	void addProvider(ParserSelectorProvider provider);

	void removeProvider(ParserSelectorProvider provider);
}
