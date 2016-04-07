package org.araqne.logdb.cep.offheap.manager;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.codec.EncodingRule;
import org.araqne.codec.FastEncodingRule;
import org.araqne.logdb.Row;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.cep.offheap.engine.Entry;
import org.araqne.logdb.cep.offheap.engine.serialize.EventKeySerialize;
import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class EventEntryStorageManager implements EntryStorageManager<EventKey, EventContext> {
	private Serialize<EventKey> keySerializer = new EventKeySerialize();
	private FastEncodingRule enc = new FastEncodingRule();
	private Unsafe unsafe;
	private final int minChunkSize;

	private enum Offset {
		Hash(0), Next(4), MaxSize(12), Created(16), ExpireTime(24), TimeoutTime(32), MaxRows(40), Counter(44), KeySize(
				48), RowsSize(52), Key(56);

		private int offset;

		private Offset(int i) {
			offset = i;
		}

		public long get(long index) {
			return index + offset;
		}
	}

	public EventEntryStorageManager(int minChunkSize) {
		this.unsafe = getUnsafe();
		this.minChunkSize = minChunkSize;
	}

	// allocate and put new entry
	public long set(Entry<EventKey, EventContext> entry) {
		int rowsSize = 0;
		int varSizes = 0;

		// encode
		EventContext ctx = entry.getValue();
		byte[] encodedKey = keySerializer.serialize(ctx.getKey());
		byte[] encodedRows = encodeRows(ctx.getRows());
		byte[] encodedVariables = encodeVars(ctx.getVariables());
		if (encodedRows != null)
			rowsSize = encodedRows.length;
		if (encodedVariables != null)
			varSizes = encodedVariables.length;

		// allocate
		int requireSize = align(sizeOf(encodedKey.length, rowsSize, varSizes));
		long address = unsafe.allocateMemory(requireSize);
		entry.setMaxSize(requireSize);

		// put data
		setHash(address, entry.getHash());
		setNext(address, entry.getNext());
		setMaxSize(address, requireSize);
		setCreated(address, ctx.getCreated());
		setExpireTime(address, ctx.getExpireTime());
		setTimeoutTime(address, ctx.getTimeoutTime());
		setMaxRows(address, ctx.getMaxRows());
		setCounter(address, ctx.getCounter().get());
		setKeySize(address, encodedKey.length);
		setRowsSize(address, rowsSize);
		setKeyByte(address, encodedKey);
		setRowsByte(address, encodedKey.length, encodedRows);
		setVariablesByte(address, requireSize, encodedVariables);

		return address;
	}

	public void setHash(long address, int hash) {
		unsafe.putInt(Offset.Hash.get(address), hash);
	}

	public void setNext(long address, long next) {
		unsafe.putLong(Offset.Next.get(address), next);
	}

	public void setEvictTime(long address, long timeoutTime) {
		unsafe.putLong(Offset.TimeoutTime.get(address), timeoutTime);
	}

	public void setTimeoutTime(long address, long timeoutTime) {
		unsafe.putLong(Offset.TimeoutTime.get(address), timeoutTime);
	}

	public void setMaxSize(long address, int maxSize) {
		unsafe.putInt(Offset.MaxSize.get(address), maxSize);
	}

	public void setKeySize(long address, int keySize) {
		unsafe.putInt(Offset.KeySize.get(address), keySize);
	}

	public void setKeyByte(long address, byte[] keyByte) {
		for (int i = 0; i < keyByte.length; i++) {
			unsafe.putByte(Offset.Key.get(address) + i, keyByte[i]);
		}
	}

	public void setKey(long address, EventKey key) {
		setKeyByte(address, keySerializer.serialize(key));
	}

	public void setValueSize(long address, int valueSize) {
		throw new UnsupportedOperationException();
	}

	public void setValueByte(long address, byte[] encodedValue) {
		throw new UnsupportedOperationException();
	}

	@Deprecated
	public void setValue(long address, EventContext ctx) {
		byte[] encodedKey = keySerializer.serialize(ctx.getKey());
		byte[] encodedRows = encodeRows(ctx.getRows());
		byte[] encodedVariables = encodeVars(ctx.getVariables());

		int rowsSize = (encodedRows == null) ? 0 : encodedRows.length;
		int variableSize = (encodedVariables == null) ? 0 : encodedVariables.length;
		int requireSize = align(sizeOf(encodedKey.length, rowsSize, variableSize));

		setCreated(address, ctx.getCreated());
		setExpireTime(address, ctx.getExpireTime());
		setTimeoutTime(address, ctx.getTimeoutTime());
		setMaxRows(address, ctx.getMaxRows());
		setCounter(address, ctx.getCounter().get());
		setKeySize(address, encodedKey.length);
		setRowsSize(address, rowsSize);
		setKeyByte(address, encodedKey);
		setRowsByte(address, encodedKey.length, encodedRows);
		setVariablesByte(address, requireSize, encodedVariables);
	}

	private void setRowsByte(long address, int offset, byte[] encodedRows) {
		if (encodedRows == null)
			return;

		for (int i = 0; i < encodedRows.length; i++) {
			unsafe.putByte(Offset.Key.get(address) + offset + i, encodedRows[i]);
		}
	}

	private void setVariablesByte(long address, int maxSize, byte[] encodedVariables) {
		long varsSizeOffset = address + maxSize - 4;
		int varsSize = 0;
		if (encodedVariables != null)
			varsSize = encodedVariables.length;

		unsafe.putInt(varsSizeOffset, varsSize);
		if (varsSize == 0)
			return;

		for (int i = 0; i < encodedVariables.length; i++) {
			unsafe.putByte(varsSizeOffset - varsSize + i, encodedVariables[i]);
		}
	}

	private void setRowsSize(long address, int rowsSize) {
		unsafe.putInt(Offset.RowsSize.get(address), rowsSize);
	}

	private void setCounter(long address, int counter) {
		unsafe.putInt(Offset.Counter.get(address), counter);
	}

	private void setMaxRows(long address, int maxRows) {
		unsafe.putInt(Offset.MaxRows.get(address), maxRows);
	}

	private void setExpireTime(long address, long expireTime) {
		unsafe.putLong(Offset.ExpireTime.get(address), expireTime);
	}

	private void setCreated(long address, long created) {
		unsafe.putLong(Offset.Created.get(address), created);
	}

	public EventContext getValue(long address) {
		long created = getCreated(address);
		long expireTime = getExpireTime(address);
		long timeoutTime = getTimeoutTime(address);
		int maxRows = getMaxRows(address);
		int counter = getCounter(address);
		int keySize = getKeySize(address);
		int rowsSize = getRowsSize(address);
		EventKey key = getKey(address);
		List<Row> rows = getRows(address, keySize, rowsSize);
		HashMap<String, Object> variables = getVariables(address, getMaxSize(address));

		EventContext ctx = new EventContext(key, created, expireTime, timeoutTime, maxRows);
		ctx.getCounter().set(counter);

		for (Row row : rows)
			ctx.addRow(row);

		for (String s : variables.keySet())
			ctx.setVariable(s, variables.get(s));

		return ctx;
	}

	public Entry<EventKey, EventContext> get(long address) {
		int hash = getHash(address);
		long next = getNext(address);
		int maxSize = getMaxSize(address);
		EventKey key = getKey(address);
		EventContext ctx = getValue(address);

		return new Entry<EventKey, EventContext>(key, ctx, hash, next, getMinValue(ctx.getExpireTime(),
				ctx.getTimeoutTime()), maxSize);
	}

	private HashMap<String, Object> getVariables(long address, int maxSize) {
		long varsSizeOffset = address + maxSize - 4;
		int size = unsafe.getInt(varsSizeOffset);
		if (size == 0)
			return new HashMap<String, Object>();

		byte[] variables = new byte[size];
		for (int i = 0; i < size; i++) {
			variables[i] = unsafe.getByte(varsSizeOffset - size + i);//
		}

		return ((HashMap<String, Object>) EncodingRule.decodeMap(ByteBuffer.wrap(variables)));
	}

	private List<Row> getRows(long address, int offset, int size) {
		if (size == 0)
			return new ArrayList<Row>();

		byte[] rows = new byte[size];
		for (int i = 0; i < size; i++) {
			rows[i] = unsafe.getByte(Offset.Key.get(address) + offset + i);
		}

		return parseRows(EncodingRule.decodeArray(ByteBuffer.wrap(rows)));
	}

	private int getMaxRows(long address) {
		return unsafe.getInt(Offset.MaxRows.get(address));
	}

	private int getCounter(long address) {
		return unsafe.getInt(Offset.Counter.get(address));
	}

	private int getVariablesSize(long address, int maxSize) {
		return unsafe.getInt(address + maxSize - 4);
	}

	private long getExpireTime(long address) {
		return unsafe.getLong(Offset.ExpireTime.get(address));
	}

	private long getCreated(long address) {
		return unsafe.getLong(Offset.Created.get(address));
	}

	public int getHash(long address) {
		return unsafe.getInt(Offset.Hash.get(address));
	}

	public long getNext(long address) {
		return unsafe.getLong(Offset.Next.get(address));
	}

	public long getEvictTime(long address) {
		long timeoutTime = getTimeoutTime(address);
		long expireTime = getExpireTime(address);
		return getMinValue(expireTime, timeoutTime);
	}

	private long getMinValue(long a, long b) {
		long min = Math.min(a, b);

		if (min > 0)
			return min;
		else
			return Math.max(a, b);
	}

	public long getTimeoutTime(long address) {
		return unsafe.getLong(Offset.TimeoutTime.get(address));
	}

	public int getKeySize(long address) {
		return unsafe.getInt(Offset.KeySize.get(address));
	}

	public int getValueSize(long address) {
		throw new UnsupportedOperationException();
	}

	public int getMaxSize(long address) {
		return unsafe.getInt(Offset.MaxSize.get(address));
	}

	private int getRowsSize(long address) {
		return unsafe.getInt(Offset.RowsSize.get(address));
	}

	public EventKey getKey(long address) {
		return keySerializer.deserialize(getKeyByte(address));
	}

	private byte[] getKeyByte(long address) {
		int keySize = unsafe.getInt(Offset.KeySize.get(address));

		byte[] keys = new byte[keySize];
		for (int i = 0; i < keySize; i++) {
			keys[i] = unsafe.getByte(Offset.Key.get(address) + i);
		}
		return keys;
	}

	public long updateValue(long address, EventContext ctx, long timeoutTime) {
		int rowsSize = 0;
		int varsSize = 0;
		byte[] encodedRows = encodeRows(ctx.getRows());
		byte[] encodedVariables = encodeVars(ctx.getVariables());

		if (encodedRows != null)
			rowsSize = encodedRows.length;

		if (encodedVariables != null)
			varsSize = encodedVariables.length;

		int keySize = getKeySize(address);
		int maxSize = getMaxSize(address);
		int size = sizeOf(keySize, rowsSize, varsSize);

		if (size < maxSize) {
			setCreated(address, ctx.getCreated());
			setExpireTime(address, ctx.getExpireTime());
			setTimeoutTime(address, ctx.getTimeoutTime());
			setMaxRows(address, ctx.getMaxRows());
			setCounter(address, ctx.getCounter().get());
			setRowsSize(address, rowsSize);
			setRowsByte(address, keySize, encodedRows);
			setVariablesByte(address, maxSize, encodedVariables);
		} else {
			// reallocate
			long oldAddress = address;
			int requireSize = align(size);
			address = unsafe.allocateMemory(requireSize);
			unsafe.copyMemory(oldAddress, address, Offset.Key.offset + keySize);

			setMaxSize(address, requireSize);
			setCreated(address, ctx.getCreated());
			setExpireTime(address, ctx.getExpireTime());
			setTimeoutTime(address, ctx.getTimeoutTime());
			setMaxRows(address, ctx.getMaxRows());
			setCounter(address, ctx.getCounter().get());
			setRowsSize(address, rowsSize);
			setRowsByte(address, keySize, encodedRows);
			setVariablesByte(address, requireSize, encodedVariables);

			unsafe.freeMemory(oldAddress);
		}

		return address;
	}

	@Override
	public long replaceValue(long address, EventContext oldValue, EventContext newValue, long evictTime) {
		// replace only if oldValue equals stored value
		int counter = getCounter(address);
		if (counter != oldValue.getCounter().get())
			return 0L;

		long created = getCreated(address);
		if (created != oldValue.getCreated())
			return 0L;

		long expireTime = getExpireTime(address);
		if (expireTime != oldValue.getExpireTime())
			return 0L;

		long timeoutTime = getTimeoutTime(address);
		if (timeoutTime != oldValue.getTimeoutTime())
			return 0L;

		int maxRows = getMaxRows(address);
		if (maxRows != oldValue.getMaxRows())
			return 0L;

		int keySize = getKeySize(address);
		int rowsSize = getRowsSize(address);
		int maxSize = getMaxSize(address);
		int variablesSize = getVariablesSize(address, maxSize);

		HashMap<String, Object> variables = getVariables(address, maxSize);
		if (!variables.equals(oldValue.getVariables()))
			return 0L;

		List<Row> rows = getRows(address, keySize, rowsSize);
		if (!oldValue.equalRowLists(rows)) {
			return 0L;
		}

		byte[] encodedRows = null;
		byte[] encodedVariables = null;
		int newRowSize = rowsSize;
		int newVarSize = variablesSize;

		if (!newValue.equalRowLists(rows)) {
			encodedRows = encodeRows(newValue.getRows());
			newRowSize = encodedRows.length;
		}

		if (!variables.equals(newValue.getVariables())) {
			encodedVariables = encodeVars(newValue.getVariables());
			newVarSize = encodedVariables.length;
		}

		// set values only if it`s changed
		int size = sizeOf(keySize, newRowSize, newVarSize);
		if (size < maxSize) {
			if (counter != newValue.getCounter().get())
				setCounter(address, newValue.getCounter().get());

			if (created != newValue.getCreated())
				setCreated(address, newValue.getCreated());

			if (expireTime != newValue.getExpireTime())
				setExpireTime(address, newValue.getExpireTime());

			if (timeoutTime != newValue.getTimeoutTime())
				setTimeoutTime(address, newValue.getTimeoutTime());

			if (maxRows != newValue.getMaxRows())
				setMaxRows(address, newValue.getMaxRows());

			if (encodedRows != null) {
				setRowsSize(address, encodedRows.length);
				setRowsByte(address, keySize, encodedRows);
			}

			if (encodedVariables != null) {
				setVariablesByte(address, maxSize, encodedVariables);
			}
		} else {
			long oldAddress = address;
			int requireSize = align(size);
			address = unsafe.allocateMemory(requireSize);
			unsafe.copyMemory(oldAddress, address, Offset.Key.offset + keySize);
			setMaxSize(address, requireSize);

			if (counter != newValue.getCounter().get())
				setCounter(address, newValue.getCounter().get());

			if (created != newValue.getCreated())
				setCreated(address, newValue.getCreated());

			if (expireTime != newValue.getExpireTime())
				setExpireTime(address, newValue.getExpireTime());

			if (timeoutTime != newValue.getTimeoutTime())
				setTimeoutTime(address, newValue.getTimeoutTime());

			if (maxRows != newValue.getMaxRows())
				setMaxRows(address, newValue.getMaxRows());

			if (encodedRows != null) {
				setRowsSize(address, encodedRows.length);
				setRowsByte(address, keySize, encodedRows);
			}

			if (encodedVariables != null) {
				setVariablesByte(address, requireSize, encodedVariables);
			}

			unsafe.freeMemory(oldAddress);
		}

		return address;
	}

	public boolean equalsHash(long address, int hash) {
		int oldHash = getHash(address);
		return hash == oldHash;
	}

	public boolean equalsKey(long address, EventKey key) {
		byte[] encodedKey = keySerializer.serialize(key);
		int keySize = getKeySize(address);

		for (int i = 0; i < keySize; i++) {
			if (encodedKey[i] != unsafe.getByte(Offset.Key.get(address) + i)) {
				return false;
			}
		}
		return true;
	}

	public void remove(long address) {
		unsafe.freeMemory(address);
	}

	private static Unsafe getUnsafe() {
		try {
			Field f = Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			return (Unsafe) f.get(null);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private byte[] encodeRows(List<Row> rows) {
		List<Map<String, Object>> rowMaps = new ArrayList<Map<String, Object>>();
		for (Row row : rows) {
			rowMaps.add(row.map());
		}

		if (rowMaps.size() == 0)
			return null;

		return enc.encode(rowMaps).array();
	}

	private byte[] encodeVars(Map<String, Object> variables) {
		if (variables.size() == 0)
			return null;

		return enc.encode(variables).array();
	}

	@SuppressWarnings("unchecked")
	private List<Row> parseRows(Object[] rowMaps) {
		List<Row> rows = Collections.synchronizedList(new ArrayList<Row>());
		for (Object rowMap : rowMaps) {
			rows.add(new Row((Map<String, Object>) rowMap));
		}
		return rows;
	}

	private int sizeOf(int keySize, int rowsSize, int varSize) {
		return Offset.Key.offset + keySize + rowsSize + varSize;
	}

	private int align(int i) {
		if (i < minChunkSize)
			return minChunkSize;

		return minChunkSize * ((i / minChunkSize) + 1);
	}

}
