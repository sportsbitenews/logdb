package org.araqne.logparser.krsyslog.jiran;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class SpamSniperParserTest {
	@Test
	public void testSpamInboundMaster() {
		String line = "seq=26016737 mail_id=1361900959_28116 sender_ip=1.2.3.4 sender_email=werwer@werwer.net receiver_email=hoho@hoho.net mail_type=2 db_name=all_deny filter_type=\"RPD Engine\" filter_action=2 filter_content=\"----\" header_subject=\"Time for perfect nights with your partner\" virus_name= iscontent=1 user_group= user_domain=wwweee.co.kr date=1361900960.000 recover_date= attach= hostname=1.2.3.4 mail_size=899 country= run_mode=1 send_id=I1361900959_28116_1";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		SpamSniperParser p = new SpamSniperParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("26016737", m.get("seq"));
		assertEquals("werwer@werwer.net", m.get("sender_email"));
		assertEquals("RPD Engine", m.get("filter_type"));
		assertEquals("----", m.get("filter_content"));
		assertEquals("", m.get("recover_date"));
		assertEquals("I1361900959_28116_1", m.get("send_id"));
	}

	@Test
	public void testSpamInrejectMaster() {
		String line = "seq=10829942 sender_ip=1.2.3.4 mail_from=werwer@hwww.com rcpt_to=hi@aaa.com reject_code=113 hostname=1.2.3.4 country= date=1361898548.000";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		SpamSniperParser p = new SpamSniperParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("10829942", m.get("seq"));
		assertEquals("1.2.3.4", m.get("sender_ip"));
		assertEquals("", m.get("country"));
		assertEquals("1361898548.000", m.get("date"));
	}
}
