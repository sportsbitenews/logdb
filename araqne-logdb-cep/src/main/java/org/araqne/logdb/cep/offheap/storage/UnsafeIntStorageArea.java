package org.araqne.logdb.cep.offheap.storage;

/*
 *  unsafe integer array ~= int[]
 */
@SuppressWarnings("restriction")
public class UnsafeIntStorageArea extends AbsractUnsafeStorageArea<Integer> {

	public UnsafeIntStorageArea() {
		super(4);
	}

	public UnsafeIntStorageArea(int initialCapacity) {
		super(4, initialCapacity);
	}

	@Override
	public void setValue(int index, Integer value) {
		storage.putInt(index(index), value);
	}

	@Override
	public Integer getValue(int index) {
		return storage.getInt(index(index));
	}

}
/*69206274 190 1
69206480 135 1
69206632 111 1
100000 allocate end : 91492  free chunk :1
# A fatal error has been detected by the Java Runtime Environment:
#
#  EXCEPTION_ACCESS_VIOLATION (0xc0000005) at pc=0x0000000002950ece, pid=27668, tid=29028
*
*
*
*68163470 51 1
68163538 97 1
68163652 53 1
68163722 47 1
68163786 84 1
100000 allocate end : 91333  free chunk :1
#
# A fatal error has been detected by the Java Runtime Environment:
#
#  EXCEPTION_ACCESS_VIOLATION (0xc0000005) at pc=0x000000000279826c, pid=31236, tid=31044



59136298 154 43477
59136468 87 43477
59136572 102 43477
59136690 133 43477
59136840 55 43477
*/