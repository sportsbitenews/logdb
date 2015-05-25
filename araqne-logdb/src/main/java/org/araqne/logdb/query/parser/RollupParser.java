package org.araqne.logdb.query.parser;

import java.util.List;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.query.aggregator.AggregationField;
import org.araqne.logdb.query.command.Stats;

public class RollupParser extends StatsParser {

	@Override
	public String getCommandName() {
		return "rollup";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		Stats stats = (Stats) super.parse(context, commandString);
		return new Rollup(stats.getAggregationFields(), stats.getClauses());
	}

	private class Rollup extends Stats {
		public Rollup(List<AggregationField> fields, List<String> clause) {
			super(fields, clause, true);
		}

		@Override
		public String getName() {
			return "rollup";
		}
	}
}
