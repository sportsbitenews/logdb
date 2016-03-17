package org.araqne.logdb.cep.offheap.storage;

import org.araqne.logdb.cep.offheap.allocator.AllocableArea;

//StorageArea 는 실제 물리적으로 저장되는 공간
//max size = 스토리지의 총크기, byte 단위
//사용자는 index 단위가 long인지 int인지 몰라야 된다...
//index는 내부적으로 자동 계산...
//max_size의 크기가 Integer.Max_value * 4(int = 4 bytes) 보다 크면 address는 long type 아니면 int type (즉 최대 크기는 long.max_value * 8 bytes)
//사용자는 index 에 원하는 type을 저장한다...
//원하는 value 타입에 따라 storage이 이름이 바뀜
//사용자가 index값을 맘대로 넣으면 충돌이 생길가능성이 높으므로 allocate가 필요
//allocate는 storage area에 meta 정보를 저장해서 저장 크기에 따라 적절한 위치를 알려줌

/*
 * 1. index의 크기의 자동조절
 * 사용자는 index를 항상 long type으로 사용하지만 실제 storage의 최대 크기가 그리 크지 않으면 long type index는 낭비이므로 자동 조절해준다.
 *  
 * 2. value type에 따른 index
 *  1. 고정길이 스토리지를 사용하는 사용자는 index당 하나의 값을 입력하려고 할것이다.
 *  2. 따라서 고정 길이 스토리지의 인덱스는 value고정크기 * n 번째 즉 value가 int일 때 index가 10이면 실제로는  시작점에서 40byte ~ 43byte 사이에 위치한 값이다.
 *  3. 가변길이는 value의 길이가 가변이므로 index의 단위는 무조건 byte이다. 즉 index가 10이면 실제로 10byte 부터 시작되는 값이다.  
 *   
 */

// byte = 1, short=2, int=4, long=8

//String area의 index 단위는 byte!
// storage크기에 따라  allocate할때 저장할 index 사이즈 조절!
// unsafe 의 offset은 putlong, putbyte등에 상관없이 무조건 byte 주소

//storagArea<String> -> string 값을 저장할 수 있음
//allocable allocator로 사용가능!

@SuppressWarnings("restriction")
public class UnsafeStringStorageArea extends AbsractUnsafeStorageArea<String> implements AllocableArea<String> {

	public UnsafeStringStorageArea() {
		super(1);
	}

	public UnsafeStringStorageArea(int initCapaciry) {
		super(1, initCapaciry);
	}

	@Override
	public String getValue(int index) {
		int valueLength = storage.getInt(index(index));
		byte[] ret = new byte[valueLength];
		for (int i = 0; i < valueLength; i++) {
			ret[i] = storage.getByte(index(index) + 4 + i); // 4 = size of
															// integer
		}
		return new String(ret);
	}

	@Override
	public void setValue(int index, String value) {
		byte[] bytes = value.getBytes();
		// XXX value size는 항상 integer?
		// byte 단위로 (signal bit 사용하면 공간 절약 but 읽고 쓰는 시간 추가 필요)
		storage.putInt(index(index), bytes.length);
		for (int i = 0; i < bytes.length; i++) {
			storage.putByte(index(index) + 4 + i, bytes[i]); // 4 = size of
																// integer
		}
	}

	@Override
	public void setAddress(int index, int address) {
		storage.putInt(index(index), (int) address);
	}

	@Override
	public int getAddress(int index) {
		return  storage.getInt(index(index));
	}

	@Override
	public int capacity() {
		return capacity;
	}

}
