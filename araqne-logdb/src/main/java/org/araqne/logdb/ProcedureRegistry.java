package org.araqne.logdb;

import java.util.List;
import java.util.Set;

public interface ProcedureRegistry {
	Set<String> getProcedureNames();

	List<Procedure> getProcedures();

	Procedure getProcedure(String name);

	void createProcedure(Procedure procedure);

	/**
	 * @since 2.4.60
	 */
	void updateProcedure(Procedure procedure);

	void removeProcedure(String name);
}
