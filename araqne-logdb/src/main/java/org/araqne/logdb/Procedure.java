package org.araqne.logdb;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.araqne.api.CollectionTypeHint;
import org.araqne.api.FieldOption;
import org.araqne.confdb.CollectionName;

@CollectionName("procedures")
public class Procedure {
	@FieldOption(nullable = false)
	private String name;

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

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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
	public String toString() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return "name=" + name + ", parameters=" + parameters + ", query=" + queryString + ", created=" + df.format(created)
				+ ", modified=" + df.format(modified) + ", owner=" + owner + ", grants=" + grants;
	}

}
