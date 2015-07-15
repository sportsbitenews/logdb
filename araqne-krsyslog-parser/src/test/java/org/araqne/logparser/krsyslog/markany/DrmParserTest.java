package org.araqne.logparser.krsyslog.markany;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class DrmParserTest {
	@Test
	public void testLogFileHistory() {
		String line = "File_ID=09010f0e8018973f Downloader_ID= User_ID=A300195 File_Name=\"GasTurbine Generator.doc\" User_IP=10.11.2.159 User_ComName=65044X박영호 Oper_Type=2 File_StoragePath= Scope_Name=미적용 Scope_ID=0 User_UID=1019528 User_Name=박영호 User_DeptCode=J9P0 User_DeptName=\"쿠웨이트 SABIYA현장\" User_PosCode=BA0 User_PosName=부장 File_Desc=\"표준 RFQ for Gas Turbine Generator\" File_RegDate=1142551010.140 ACL_DocLevelName=대외비 File_DocLevel=1 sFlag=EDMS User_BzCode=\"AJ00    \" User_BsCode=J9P0 Server_origin=EDMS01";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		DrmParser p = new DrmParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("09010f0e8018973f", m.get("file_id"));
		assertEquals("", m.get("downloader_id"));
		assertEquals("A300195", m.get("user_id"));
		assertEquals("GasTurbine Generator.doc", m.get("file_name"));
		assertEquals("65044X박영호", m.get("user_comname"));
		assertEquals("AJ00    ", m.get("user_bzcode"));
		assertEquals("EDMS01", m.get("server_origin"));
	}

	@Test
	public void testDocTakeout() {
		String line = "USER_ID=A390089 User_Name=김진신 Dept_Name=변압기설계부 Pos_Name=차장 Request_Reason=\"고객 승인용 제출 도면임.\" Answer_Reason=승인합니다. Approval_Name=김진신 Approval_UID=A390089 SignDate=1357274892.397 APP_IP=10.16.51.31 File_Name=TL2381-L05A-A1-R06.dwg";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		DrmParser p = new DrmParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("A390089", m.get("user_id"));
		assertEquals("김진신", m.get("user_name"));
		assertEquals("고객 승인용 제출 도면임.", m.get("request_reason"));
		assertEquals("TL2381-L05A-A1-R06.dwg", m.get("file_name"));
	}

}
