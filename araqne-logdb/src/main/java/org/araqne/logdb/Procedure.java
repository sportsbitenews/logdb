package org.araqne.logdb;

import java.text.SimpleDateFormat;
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
	private List<String> grants;

	@CollectionTypeHint(ProcedureVariable.class)
	private List<ProcedureVariable> variables;

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

	public List<ProcedureVariable> getVariables() {
		return variables;
	}

	public void setVariables(List<ProcedureVariable> variables) {
		this.variables = variables;
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
		return "name=" + name + ", variables=" + variables + ", queryString=" + queryString + ", created=" + df.format(created)
				+ ", modified=" + df.format(modified) + ", owner=" + owner + ", grants=" + grants;
	}

}
