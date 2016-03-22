package org.araqne.logdb.sort;

import java.io.IOException;
import java.net.Inet4Address;
import java.util.Comparator;
import java.util.Random;

public class ParallelMergeSorterDebugging {
	public static void main(String[] args) throws IOException, InterruptedException {
		sampleRun(args);
	}
	
	public static class ItemComparer implements Comparator<Item> {
		@Override
		public int compare(Item o1, Item o2) {
			String os1 = (String) o1.key;
			String os2 = (String) o2.key;
			return os1.compareTo(os2);
		}
	}
	
	public static void sampleRun(String[] args) throws IOException, InterruptedException {
		ParallelMergeSorter sorter = new ParallelMergeSorter(new ItemComparer(), 10);

		Random rand1 = new Random(1);
		
		byte[] addr = new byte[4]; 
		for (int i = 0; i < 100000; ++i) {
			rand1.nextBytes(addr);
			String addrStr = Inet4Address.getByAddress(addr).getHostAddress();
			sorter.add(new Item(addrStr, null));
		}
		CloseableIterator sorted = sorter.sort();
		
		int resultCnt = 0;
		String last = null;
		while (sorted.hasNext()) {
			String current = (String) sorted.next().key;
			
			if (last != null) {
				if (last.compareTo(current) > 0)
					System.out.println("ERROR");
			}
			
			last = current;
		}
		sorted.close();
	}
}
