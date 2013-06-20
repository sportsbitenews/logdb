package org.araqne.logdb.query.expr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.araqne.logdb.LogMap;

public class EqTest {
	public static void main(String[] args) throws IOException {
		BufferedReader in = new BufferedReader(new FileReader(args[0]));

		String l = null;
		List<LogMap> logs = new ArrayList<LogMap>();

		long startTime = System.currentTimeMillis();
		long maxline = 1000000;
		long lc = 0;
		while (lc < maxline && (l = in.readLine()) != null) {
			LogMap log = new LogMap();
			log.put("line", l);
			logs.add(log);
			lc++;
		}
		in.close();
		System.out.println(System.currentTimeMillis() - startTime);

		// match test
		List<StringConstant> patterns = new ArrayList<StringConstant>();
		patterns.add(new StringConstant("192"));
		patterns.add(new StringConstant("192*"));
		patterns.add(new StringConstant("*192"));
		patterns.add(new StringConstant("*192*"));
		patterns.add(new StringConstant("*192*0*"));
		
		for (StringConstant pattern : patterns) {
		long oldtime = 0;
		long newtime = 0;
		startTime = System.currentTimeMillis();
		EvalField line = new EvalField("line");
		Eq eq = new Eq(line, pattern);
		for (LogMap log : logs) {
			eq.eval(log);
		}
		oldtime = System.currentTimeMillis() - startTime;

		startTime = System.currentTimeMillis();
		Eq.NewEq neweq = new Eq.NewEq(line, pattern);
		for (LogMap log : logs) {
			neweq.eval(log);
		}
		newtime = System.currentTimeMillis() - startTime;
		
		System.out.printf("Pattern - %s\tOld : %d , New: %s . %f%%\n", pattern, oldtime, newtime, (double)oldtime/(double)newtime*100);
		}
	}
}