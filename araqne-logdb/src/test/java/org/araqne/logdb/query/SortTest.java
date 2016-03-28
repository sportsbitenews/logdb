package org.araqne.logdb.query;

import java.io.IOException;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;

import org.junit.Ignore;
import org.araqne.logdb.ObjectComparator;
import org.araqne.logdb.sort.Item;
import org.araqne.logdb.sort.ParallelMergeSorter;

@Ignore
public class SortTest {
	public static void main(String[] args) throws Exception {
		new SortTest().run();
	}

	public void run() throws IOException {
		ParallelMergeSorter sorter = new ParallelMergeSorter(new ItemComparer());

		long begin = System.currentTimeMillis();

		int max = 10000000;
		for (int i = max; i >= 0; i--) {
			sorter.add(new Item(i, null));
			if (i % 10000000 == 0)
				System.out.println(i + " passed");
		}
		System.out.println("push complete " + (System.currentTimeMillis() - begin) + "ms");

		Iterator<Item> it = sorter.sort();
		System.out.println("start result check");
		int i = 0;
		boolean suppress = false;
		while (it.hasNext()) {
			Item item = it.next();
			Object next = item.key;
			if (!next.equals(i) && !suppress) {
				System.out.println("###################### bug check: " + next + ", " + i);
				suppress = true;
			}
			i++;
		}

		if (i <= max)
			System.out.println("############ bug check, last " + i);

		System.out.println("count: " + i);

		long elapsed = new Date().getTime() - begin;
		System.out.println("elapsed " + elapsed);
	}

	private static class ItemComparer implements Comparator<Item> {
		private ObjectComparator cmp = new ObjectComparator();

		@Override
		public int compare(Item o1, Item o2) {
			return cmp.compare(o1.key, o2.key);
		}
	}
}
