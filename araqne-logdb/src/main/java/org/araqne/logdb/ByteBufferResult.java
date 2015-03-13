package org.araqne.logdb;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.araqne.codec.EncodingRule;
import org.araqne.storage.api.RCDirectBuffer;
import org.araqne.storage.api.RCDirectBufferManager;

public class ByteBufferResult implements QueryResult{
	private RCDirectBuffer directBuffer;
	private int writePosition;
	

	ByteBufferResult(QueryResultConfig config, RCDirectBufferManager directBufferManager, int capacity) {
		this.directBuffer = directBufferManager.allocateDirect(capacity);
		this.writePosition = 0;
	}
	
	@Override
	public boolean isThreadSafe() {
		return false;
	}

	@Override
	public void onRow(Row row) {
		writeLog(row.map());
	}
	

	private int count = 0;
	private void writeLog(Map<String, Object> log) {
		EncodingRule.encode(directBuffer.get(), log);
		this.writePosition = directBuffer.get().position();
	}

	@Override
	public void onRowBatch(RowBatch rowBatch) {
		for(Row row : rowBatch.rows) {
			onRow(row);
		}
	}

	@Override
	public Date getEofDate() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getCount() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void syncWriter() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void closeWriter() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void purge() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isStreaming() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setStreaming(boolean streaming) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public QueryResultSet getResultSet() throws IOException {
		return (QueryResultSet) new ByteBufferResultSet(this.directBuffer, this.writePosition);
	}

	@Override
	public Set<QueryResultCallback> getResultCallbacks() {
		// TODO Auto-generated method stub
		return null;
	}

	public class ByteBufferResultSet implements QueryResultSet {
		private ByteBuffer byteBuffer;
		private int lastPosition;

		private ByteBufferResultSet(RCDirectBuffer directBuffer, int lastPosition) {
			directBuffer.addRef();
			byteBuffer = directBuffer.get().asReadOnlyBuffer();
			byteBuffer.position(0);
			this.lastPosition = lastPosition;
		}
	
		public int getPosition() {
			return byteBuffer.position();
		}
		
		public ByteBuffer getByteBuffer() {
			return this.byteBuffer;
		}
		
		@Override
		public boolean hasNext() {
			return (byteBuffer.position() < lastPosition);
		}

		@Override
		public Map<String, Object> next() {
			return (Map<String, Object>) EncodingRule.decode(byteBuffer);
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public String getStorageName() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public File getIndexPath() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public File getDataPath() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public long size() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void skip(long n) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void reset() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			
		}
	}
}
