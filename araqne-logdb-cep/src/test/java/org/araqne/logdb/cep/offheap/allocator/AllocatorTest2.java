package org.araqne.logdb.cep.offheap.allocator;

import org.junit.Test;

public class AllocatorTest2 {
	
	@Test
	public void test() {
	}
//
//	public static void main(String[] args) throws Exception {
//		Random random = new Random();
//
//		UnsafeByteArrayStorageArea storage = new UnsafeByteArrayStorageArea(Integer.MAX_VALUE);
//		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storage);
//		int userSize = 1000000;
//		// BlockingQueue<Integer> addrs = new
//		// ArrayBlockingQueue<Integer>(50000000);
//		BlockingQueue<Integer> addrs = new ArrayBlockingQueue<Integer>(50000000);
//
//		int[] requires = new int[userSize];
//
//		for (int i = 0; i < requires.length; i++) {
//			requires[i] = Rand.next(random, 10, 500);
//		}
//
//		System.out.println("start!");
//
//		for (int i = 0; i < 10; i++) {
//			for (int require : requires) {
//				if (Rand.nextBoolean(random)) {
//					addrs.put(allocator.allocate(require));
//				}
//			}
//		}
//
//		System.out.println(userSize * 10 + " base end");
//		for (int k = 0; k < 10; k++) {
//			long s = System.currentTimeMillis();
//			for (int require : requires) {
//				if (Rand.nextBoolean(random)) {
//					int a = allocator.allocate(require);
//					if (a == -1) {
//						throw new IllegalArgumentException();
//					}else {
//						//System.out.println("address : " + a + " size : " + require + " chunk " + allocator.freeChunk());
//					}
//					addrs.add(a);
//				}
//			}
//			long e = System.currentTimeMillis() - s;
//			System.out.println(userSize + " allocate end : " + e + "  free chunk :" + allocator.freeChunk());
//
//			long s2 = System.currentTimeMillis();
//			for (int i = 0; i < addrs.size(); i++) {
//				int address = addrs.poll();
//				if (Rand.nextBoolean(random, userSize / 2, addrs.size())) {
//					allocator.free(address);
//				} else {
//					addrs.add(address);
//				}
//			}
//
//			long e2 = System.currentTimeMillis() - s2;
//			System.out.println("free end : " + e2 + "  " + addrs.size() +  " allocate end : " + e + "  free chunk :" + allocator.freeChunk());
//		}
//
//		storage.close();
//
//		// long s = System.currentTimeMillis();
//		// int size = 0;
//		// for(int i = 0; i < 1000000;i++)
//		// size = Rand.next(1, 200);
//		// long e = System.currentTimeMillis() - s;
//		// System.out.println(e);
//
//		// ExecutorService executor = Executors.newScheduledThreadPool(10);
//		// List<Long> addr = new CopyOnWriteArrayList<Long>();
//		//
//
//		//
//		// long s = System.currentTimeMillis();
//		//
//		//
//		//
//		//
//		// for (int i = 0; i < 1000; i++) {
//		// executor.submit(new UserCallable(new User("user" + i, allocator,
//		// addr), 100));
//		// }
//		//
//		// executor.shutdown();
//		//
//		// try {
//		// executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
//		// } catch (InterruptedException e) {
//		// }
//		//
//		// long e = System.currentTimeMillis() - s;
//		// System.out.println(e + " " + addr.size());
//		//
//		// storage.close();
//		//
//		// for(int i = 0; i < addr.size();i++) {
//		//
//		// // System.out.println(i +" : " + addr.get(i));
//		// }
//		//
//		//
//		//
//		//
//		// System.out.println("end");

}
