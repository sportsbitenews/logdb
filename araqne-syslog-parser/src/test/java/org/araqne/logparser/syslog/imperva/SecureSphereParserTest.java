package org.araqne.logparser.syslog.imperva;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class SecureSphereParserTest {
	@Test
	public void test() {
		String line = "SecureSphere|629384|2015-12-10 18:15:54.0|Cookie Tampering on cookie UI: Expected , Observed 0=20111755&1=w+Yw&2=210.109.76.82&3=tjdfyd101@wer.co.kr&4=tjdfyd101&5=C7340&6=wer&7=JWGROUP&8=JWwerr&9=20111755,top,hh,C6000,C0020,C7000,C7800,C7340&10=c=0;a=0;k=0;i=0;g=0;l=0;n=0;m=0;w=0;t=0;h=0;x=0;b=0;&11=010-9915-8369+++++++&12=&13=사원&14=&LANG=&15=235|+09:00&16=JW생명과학&17=JWt&18=e&19=e&20=a&21=ww&22=사원&23=사원&24=C|Medium|1.2.3.4|49043|5.6.7.8|80|None|1|wer.co.kr|/login.aspx";
		Map<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		SecureSphereParser p = new SecureSphereParser();
		Map<String, Object> m = p.parse(log);

		assertEquals(m.get("device"), "SecureSphere");
		assertEquals(m.get("id"), "629384");
		assertEquals(m.get("detect_time"), "2015-12-10 18:15:54.0");
		assertEquals(m.get("risk"), "Medium");
		assertEquals(m.get("src_ip"), "1.2.3.4");
		assertEquals(m.get("src_port"), "49043");
		assertEquals(m.get("dst_ip"), "5.6.7.8");
		assertEquals(m.get("dst_port"), "80");
	}
}
