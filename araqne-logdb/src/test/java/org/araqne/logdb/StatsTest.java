package org.araqne.logdb;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.query.aggregator.AbstractAggregationFunction;
import org.araqne.logdb.query.aggregator.AggregationFunction;
import org.araqne.logdb.query.aggregator.CorrelationCoefficient;
import org.araqne.logdb.query.aggregator.Covariance;
import org.araqne.logdb.query.aggregator.Slope;
import org.araqne.logdb.query.aggregator.Variance;
import org.araqne.logdb.query.command.IoHelper;
import org.araqne.logdb.query.expr.EvalField;
import org.araqne.logdb.query.expr.Expression;
import org.junit.BeforeClass;
import org.junit.Test;

import au.com.bytecode.opencsv.CSVReader;

public class StatsTest {
	private double VALID_ERROR = 0.02;

	private static ArrayList<Row> rows;

	@BeforeClass
	public static void beforeClass() {
		rows = readCsv("./.statsTest/covar.csv");
	}

	private static ArrayList<Row> readCsv(String filePath) {
		FileInputStream is = null;
		CSVReader reader = null;

		ArrayList<Row> result = new ArrayList<Row>();
		try {
			File f = new File(filePath);
			is = new FileInputStream(f);
			reader = new CSVReader(new InputStreamReader(is, "utf-8"), ',', '\"', '\0');
			String[] headers = reader.readNext();
			int headerCount = headers.length;
			while (true) {
				String[] items = reader.readNext();
				if (items == null)
					break;

				int itemCount = items.length;

				Map<String, Object> m = new HashMap<String, Object>();
				for (int i = 0; i < Math.min(headerCount, itemCount); i++) {
					m.put(headers[i], Double.valueOf(items[i]));
				}

				if (itemCount > headerCount) {
					for (int i = headerCount; i < itemCount; i++) {
						m.put("column" + i, items[i]);
					}
				}

				m.put("_file", f.getName());

				result.add(new Row(m));

			}

			return result;

		} catch (Throwable t) {
			throw new RuntimeException("csvfile load failure", t);
		} finally {
			IoHelper.close(reader);
			IoHelper.close(is);
		}
	}

	private void singleNodeTest(AbstractAggregationFunction function, Double expected) {
		for (Row row : rows)
			function.apply(row);

		Double n = ((Number) function.eval()).doubleValue();
		assertTrue(Math.abs(n - expected) < VALID_ERROR);
	}

	private void federationTest(AbstractAggregationFunction function, Double expected) {
		AggregationFunction mapper1 = function.mapper(function.getArguments());
		AggregationFunction mapper2 = function.mapper(function.getArguments());
		AggregationFunction mapper3 = function.mapper(function.getArguments());
		for (int i = 0; i < rows.size(); i++) {
			Row row = rows.get(i);
			if (i % 3 == 0) {
				mapper1.apply(row);
			} else if (i % 3 == 1) {
				mapper2.apply(row);
			} else {
				mapper3.apply(row);
			}
		}

		Map<String, Object> mapperEval1 = new HashMap<String, Object>();
		Map<String, Object> mapperEval2 = new HashMap<String, Object>();
		Map<String, Object> mapperEval3 = new HashMap<String, Object>();

		mapperEval1.put("result", mapper1.eval());
		mapperEval2.put("result", mapper2.eval());
		mapperEval3.put("result", mapper3.eval());

		List<Expression> reducerExpr = new ArrayList<Expression>();
		reducerExpr.add(new EvalField("result"));
		AggregationFunction reducer = function.reducer(reducerExpr);

		reducer.apply(new Row(mapperEval1));
		reducer.apply(new Row(mapperEval2));
		reducer.apply(new Row(mapperEval3));

		Double n = ((Number) reducer.eval()).doubleValue();
		assertTrue(Math.abs(n - expected) < VALID_ERROR);
	}

	@Test
	public void varianceTest() {
		List<Expression> exprs = new ArrayList<Expression>();
		exprs.add(new EvalField("a"));
		Variance variance = new Variance(exprs);
		singleNodeTest(variance, 8482076.706);
		federationTest(variance, 8482076.706);
	}

	@Test
	public void covarianceTest() {
		List<Expression> exprs = new ArrayList<Expression>();
		exprs.add(new EvalField("a"));
		exprs.add(new EvalField("b"));
		Covariance covar = new Covariance(exprs);
		singleNodeTest(covar, -964297.6126);
		federationTest(covar, -964297.6126);
	}

	@Test
	public void correlationCoefficientTest() {
		List<Expression> exprs = new ArrayList<Expression>();
		exprs.add(new EvalField("a"));
		exprs.add(new EvalField("b"));
		CorrelationCoefficient correl = new CorrelationCoefficient(exprs);
		singleNodeTest(correl, -0.1113333664);
		federationTest(correl, -0.1113333664);
	}

	@Test
	public void slopeTest() {
		List<Expression> exprs = new ArrayList<Expression>();
		exprs.add(new EvalField("a"));
		exprs.add(new EvalField("b"));
		Slope slope = new Slope(exprs);
		singleNodeTest(slope, -0.1113333664);
		federationTest(slope, -0.1113333664);
	}
}
