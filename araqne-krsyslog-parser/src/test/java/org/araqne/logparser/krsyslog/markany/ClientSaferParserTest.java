package org.araqne.logparser.krsyslog.markany;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class ClientSaferParserTest {
	@Test
	public void testLogFileHistory() {
		String line = "File_ID=09010f0e8018973f Downloader_ID= User_ID=A300195 File_Name=\"noname.doc\" User_IP=1.2.3.4 User_ComName=아무개 Oper_Type=2 File_StoragePath= Scope_Name=미적용 Scope_ID=0 User_UID=1019528 User_Name=아무개 User_DeptCode=J9P0 User_DeptName=\"부서1\" User_PosCode=BA0 User_PosName=부장 File_Desc=\"요약\" File_RegDate=1142551010.140 ACL_DocLevelName=대외비 File_DocLevel=1 sFlag=EDMS User_BzCode=\"AJ00    \" User_BsCode=J9P0 Server_origin=EDMS01";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		ClientSaferParser p = new ClientSaferParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("09010f0e8018973f", m.get("file_id"));
		assertEquals("", m.get("downloader_id"));
		assertEquals("A300195", m.get("user_id"));
		assertEquals("noname.doc", m.get("file_name"));
		assertEquals("아무개", m.get("user_comname"));
		assertEquals("AJ00    ", m.get("user_bzcode"));
		assertEquals("EDMS01", m.get("server_origin"));
	}

	@Test
	public void testDocTakeout() {
		String line = "USER_ID=A390089 User_Name=아무개 Dept_Name=부서1 Pos_Name=차장 Request_Reason=\"이유1\" Answer_Reason=승인합니다. Approval_Name=아무개 Approval_UID=A390089 SignDate=1357274892.397 APP_IP=1.2.3.4 File_Name=wer.file";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		ClientSaferParser p = new ClientSaferParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("A390089", m.get("user_id"));
		assertEquals("아무개", m.get("user_name"));
		assertEquals("이유1", m.get("request_reason"));
		assertEquals("wer.file", m.get("file_name"));
	}

}
