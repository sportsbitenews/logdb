package org.araqne.logdb;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.api.CollectionTypeHint;
import org.araqne.api.FieldOption;
import org.araqne.confdb.CollectionName;
import org.araqne.msgbus.Marshalable;
import org.araqne.msgbus.Marshaler;

@CollectionName("procedures")
public class Procedure implements Marshalable {
	@FieldOption(nullable = false)
	private String name;

	@FieldOption(nullable = true)
	private String description;

	@FieldOption(nullable = false)
	private String owner;

	@CollectionTypeHint(String.class)
	private List<String> grants = new ArrayList<String>();

	@CollectionTypeHint(ProcedureParameter.class)
	private List<ProcedureParameter> parameters = new ArrayList<ProcedureParameter>();

	@FieldOption(nullable = false)
	private String queryString;

	@FieldOption(nullable = false)
	private Date created = new Date();

	@FieldOption(nullable = false)
	private Date modified = new Date();

	public boolean isGranted(String loginName) {
		if (loginName == null)
			throw new IllegalArgumentException("login name should not be null");

		return loginName.equals(owner) || grants.contains(loginName);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public List<String> getGrants() {
		return grants;
	}

	public void setGrants(List<String> grants) {
		this.grants = grants;
	}

	public List<ProcedureParameter> getParameters() {
		return parameters;
	}

	public void setParameters(List<ProcedureParameter> parameters) {
		this.parameters = parameters;
	}

	public String getQueryString() {
		return queryString;
	}

	public void setQueryString(String queryString) {
		this.queryString = queryString;
	}

	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

	public Date getModified() {
		return modified;
	}

	public void setModified(Date modified) {
		this.modified = modified;
	}

	@Override
	public Map<String, Object> marshal() {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("name", name);
		m.put("owner", owner);
		m.put("grants", grants);
		m.put("parameters", Marshaler.marshal(parameters));
		m.put("query_string", queryString);
		m.put("created", created);
		m.put("modified", modified);
		m.put("description", description);
		return m;
	}

	@Override
	public String toString() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return "name=" + name + ", parameters=" + parameters + ", query=" + queryString + ", created=" + df.format(created)
				+ ", modified=" + df.format(modified) + ", owner=" + owner + ", grants=" + grants;
	}
}
