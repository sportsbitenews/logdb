package org.araqne.logdb.query.expr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.araqne.logdb.LogMap;

public class InTest {
	public static void main(String[] args) throws IOException {
		BufferedReader infile = new BufferedReader(new FileReader(args[0]));

		String l = null;
		List<LogMap> logs = new ArrayList<LogMap>();

		long startTime = System.currentTimeMillis();
		long maxline = 2000000;
		long lc = 0;
		while (lc < maxline && (l = infile.readLine()) != null) {
			LogMap log = new LogMap();
			log.put("line", l);
			logs.add(log);
			lc++;
		}
		infile.close();
		System.out.println(System.currentTimeMillis() - startTime);

		// match test
		List<List<Expression>> patterns = new ArrayList<List<Expression>>();
		patterns.add(Arrays.asList(new Expression[] {
				new StringConstant("192"), 
				new StringConstant("192*"), 
				new StringConstant("*192"),
				new StringConstant("*192*"),
				new StringConstant("*192*0*")		
		}));

		patterns.add(Arrays.asList(new Expression[] {
				new StringConstant("W3SVC1"), 
				new StringConstant("192"), 
				new StringConstant("strawlv"),
				new StringConstant("GET"),
				new StringConstant("HTTP")		
		}));
		
		String[] tablenames = new String[] {"secure_event_18708","secure_event_18688","secure_event_18689","secure_event_17747","secure_event_17748","secure_event_17745","secure_event_17749","secure_event_17750","secure_event_16656","secure_event_16657","secure_event_16717","secure_event_18716","secure_event_18715","secure_event_22274","secure_event_16685","secure_event_16641","secure_event_16602","secure_event_16604","secure_event_16705","secure_event_18711","secure_event_18712","secure_event_18713","secure_event_18717","secure_event_16700","secure_event_18696","secure_event_18697","secure_event_16684","secure_event_18692","secure_event_18693","secure_event_18690","secure_event_18691","secure_event_18694","secure_event_18695","secure_event_18710","secure_event_18709","secure_event_18699","secure_event_18698","secure_event_16639","secure_event_16681","secure_event_16619","secure_event_16683","secure_event_16710","secure_event_16712","secure_event_16569","secure_event_16731","secure_event_16732","secure_event_16724","secure_event_16725","secure_event_16601","secure_event_16758","secure_event_16760","secure_event_16721","secure_event_16689","secure_event_16727","secure_event_16638","secure_event_16723","secure_event_16691","secure_event_16591","secure_event_16560","secure_event_16678","secure_event_16643","secure_event_19201","secure_event_16722","secure_event_19202","secure_event_16740","secure_event_19203","secure_event_16698","secure_event_19204","secure_event_16690","secure_event_16728","secure_event_16697","secure_event_16742","secure_event_16592","secure_event_16593","secure_event_16648","secure_event_16649","secure_event_16623","secure_event_16624","secure_event_16565","secure_event_16650","secure_event_19016","secure_event_16603","secure_event_16702","secure_event_19014","secure_event_19015","secure_event_19018","secure_event_19019","secure_event_19020","secure_event_19021","secure_event_16570","secure_event_16701","secure_event_19017","secure_event_32274","secure_event_35294","secure_event_35295","secure_event_16653","secure_event_16699","secure_event_16647","secure_event_16621","secure_event_16622","secure_event_16596","secure_event_16735","secure_event_16612","secure_event_16692","secure_event_16608","secure_event_16667","secure_event_16662","secure_event_16628","secure_event_16629","secure_event_16668","secure_event_16630","secure_event_16597","secure_event_16578","secure_event_16633","secure_event_16598","secure_event_16599","secure_event_16600","secure_event_16613","secure_event_16644","secure_event_16579","secure_event_16594","secure_event_16618","secure_event_16677","secure_event_16696","secure_event_16729","secure_event_16595","secure_event_16575","secure_event_16614","secure_event_19693","secure_event_16746","secure_event_16745","secure_event_16636","secure_event_22505","secure_event_16615","secure_event_16611","secure_event_16627","secure_event_16670","secure_event_35287","secure_event_35288","secure_event_35292","secure_event_35293","secure_event_16561","secure_event_16642","secure_event_19676","secure_event_16720","secure_event_16617","secure_event_16736","secure_event_16645","secure_event_16718","secure_event_16674","secure_event_16693","secure_event_16589","secure_event_16634","secure_event_16651","secure_event_16744","secure_event_16741","secure_event_16743","secure_event_16695","secure_event_16605","secure_event_16563","secure_event_16686","secure_event_16606","secure_event_35276","secure_event_35275","secure_event_16616","secure_event_16734","secure_event_16660","secure_event_16726","secure_event_16631","secure_event_16672","secure_event_16567","secure_event_16654","secure_event_16568","secure_event_16646","secure_event_16730","secure_event_16652","secure_event_16590","secure_event_16588","secure_event_16675","secure_event_16709","secure_event_16762","secure_event_16755","secure_event_16707","secure_event_16682","secure_event_16761","secure_event_16738","secure_event_16607","secure_event_16583","secure_event_16679","secure_event_36888","secure_event_36877","secure_event_16739","secure_event_16609","secure_event_16577","secure_event_16626","secure_event_16714","secure_event_16713","secure_event_16733","secure_event_16610","secure_event_16694","secure_event_16669","secure_event_16632","secure_event_16673","secure_event_16635","secure_event_16573","secure_event_35075","secure_event_35289","secure_event_35290","secure_event_35291","secure_event_16757","secure_event_16759"};
		List<Expression> texps = new ArrayList<Expression>(tablenames.length);
		for (String t : tablenames) 
			texps.add(new StringConstant(t));
		patterns.add(texps);
		
		for (List<Expression> pattern : patterns) {
		long oldtime = 0;
		long newtime = 0;
		
		EvalField line = new EvalField("line");
		List<Expression> inargs = new ArrayList<Expression>(pattern.size()+1);
		inargs.add(line);
		inargs.addAll(pattern);
		startTime = System.currentTimeMillis();
		In in = new In(inargs);
		for (LogMap log : logs) {
			in.eval(log);
		}
		oldtime = System.currentTimeMillis() - startTime;

		startTime = System.currentTimeMillis();
		In.NewIn newin = new In.NewIn(inargs);
		for (LogMap log : logs) {
			newin.eval(log);
		}
		newtime = System.currentTimeMillis() - startTime;
		
		System.out.printf("Pattern - %s\tOld : %d , New: %s . %f%%\n", pattern, oldtime, newtime, (double)oldtime/(double)newtime*100);
		}
	}
}