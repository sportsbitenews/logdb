package org.araqne.logdb.client;

import java.util.List;

/**
 * @author kyun InsertFailCallBack 예시 - Insert 가 실패하면 실패한 테이블과 Row를 출력하는 동작
 */
public class FailureListenerPrint implements FailureListener {

	@Override
	public void onInsertFailure(String tableName, List<Row> rows, Throwable e) {
		for (Row r : rows)
			System.out.println("Insert fail : " + e.toString() + " " + tableName + " - " + r);
	}
}
