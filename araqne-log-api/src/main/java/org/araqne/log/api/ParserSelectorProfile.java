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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.api.FieldOption;
import org.araqne.confdb.CollectionName;

/**
 * @since 3.4.7
 */
@CollectionName("parser_selector_profiles")
public class ParserSelectorProfile {
	@FieldOption(nullable = false)
	private String name;

	@FieldOption(nullable = false)
	private String providerName;

	private Map<String, String> configs = new HashMap<String, String>();

	private List<ParserSelectorPredicate> predicates = new ArrayList<ParserSelectorPredicate>();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getProviderName() {
		return providerName;
	}

	public void setProviderName(String providerName) {
		this.providerName = providerName;
	}

	public Map<String, String> getConfigs() {
		return configs;
	}

	public void setConfigs(Map<String, String> configs) {
		this.configs = configs;
	}

	public List<ParserSelectorPredicate> getPredicates() {
		return predicates;
	}

	public void setPredicates(List<ParserSelectorPredicate> predicates) {
		this.predicates = predicates;
	}

	@Override
	public String toString() {
		return "name=" + name + ", provider=" + providerName + ", configs=" + configs + ", predicates=" + predicates;
	}
}
