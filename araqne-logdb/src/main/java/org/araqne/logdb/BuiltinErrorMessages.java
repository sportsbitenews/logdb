//package org.araqne.logdb;
//
//import java.util.HashMap;
//import java.util.Locale;
//import java.util.Map;
//
//public class BuiltinErrorMessages {
//	public static final  Map<String, QueryErrorMessage> errors;
//	
//	static {
//		errors = new HashMap<String, QueryErrorMessage>();
//		
////		add("10000", "no-read-permission, admin only", "권한이 없습니다. 관리자 권한이 필요합니다.");
////		add("10001",  "missing-confdb-op", "입력된 옵션 값이 없습니다.");
////		add("10002", "missing-confdb-dbname", "검색 할 컬렉션의 데이타베이스 이름을 입력하십시오.");
////		add("10003", "missing-confdb-colname", "검색 할 설정 문서의 데이타베이스 이름을 입력하십시오.");
////		add("10004", "invalid-confdb-op : [option] ", "[op]는 지원하지 않는 옵션 입니다.");
////		add("10200", "json 문자열은 큰 따옴표(\")로 시작하고 끝나야 합니다.", "missing-json-quotation");
////		add("10201", "json 형태의 문자열을 입력하십시오.", "invalid-json-type");
////		add("10202", "json 파싱에 실패하였습니다. [msg]", "invalid-json");
////		add("10400", "할당자(=) 가 없습니다.", "assign-token-not-found");
////		add("10401", "올바르지 않는 필드 이름입니다.", "field-name-not-found");
////		add("10402", "올바르지 않는 표현식입니다.", "expression-not-found");
////		add("10600", "저장소가 닫혀 있습니다.", "archive-not-opened");
////		add("10601", "offset 값은 0보다 크거나 같아야 합니다: 입력값=[offset].", "negative-offset=[offset]");
////		add("10602", "입력값이 허용 범위를 벗어났습니다: limit=[offset].", "negative-limit");
////		
////		add("10603", "[options]에서 [exp] 잘못된 옵션입니다.", "invalid-table-spec");
////		add("10604", "", "no-table-data-source");
////		add("10605", "테이블 [table]이(가) 존재하지 않습니다.", "table-not-found");
////		add("10606
////		ko:
////		", "no-read-permission"
////		add("10606
////		ko:
////		", "table-not-found"
////		add("10606
////		ko:
////		", "no-read-permission"
////		add("10700
////		", "[file]이 존재하지 않거나 읽을수 없습니다.
////		", "invalid-textfile-path");
////		"
////		add("10800
////		", "파일 경로가 유효하지 않습니다:[filepath]
////		", "zipfile [filepath] not found");
////		"
////		add("10801
////		", "파일을 읽을 수 없습니다. 권한을 확인하세요:[filepath].
////		", "cannot read zipfile [filepath], check read permission");
////		"
////		add("10802
////		", "로그파서를 찾을 수 없습니다:[parserName].
////		", """log parser not found: "" + [parserName]");
////		"
////		add("10900
////		", "[file]이 존재하지 않거나 읽을수 없습니다
////		", "invalid-json");
////		"
////		add("10901
////		", "로그파서를 찾을 수 없습니다:[parserName].
////		", """log parser not found: "" + [parserName]");
////		"
////		add("11000
////		", "프로시저를 찾을 수 없습니다. 
////		", "procedure-not-found");
////		"
////		add("11001
////		", "프로시저 변수가 타입이 맞지 않습니다. [param]는 [type] 타입이여야 합니다. 
////		", "procedure-variable-type-mismatch [Type]");
////		"
////		add("11002
////		", "프로시저 소유자를 찾을 수 없습니다. 
////		", "procedure-owner-not-found");
////		"
////		add("11003
////		", "프로시저의 인자 수가 맞지 않습니다. [preset]개의 인자가 필요한데 [params]개의 인자가 입력 됐습니다.
////		", "procedure-parameter-mismatch");
////		"
//
//		
//		
//		
//		
//	}
//	
//	public static String format(String code, Locale locale, Map<String, Object> params) 
//	{
//		return "";
//	}
//	
//	static void add(String code, String en, String ko) {
//		errors.put(code, new QueryErrorMessage(en, ko));
//	}
//}
