package org.araqne.logdb.sort;

import java.io.IOException;
import java.util.Comparator;

import static org.junit.Assert.*;
import org.junit.Test;
import org.araqne.logdb.ObjectComparator;

public class ParallelMergeSorterTest {
	@Test
	public void testEmptySort() throws IOException {
		ParallelMergeSorter sorter = new ParallelMergeSorter(new ItemComparer());
		CloseableIterator it = sorter.sort();
		assertFalse(it.hasNext());
	}

	private static class ItemComparer implements Comparator<Item> {
		private ObjectComparator cmp = new ObjectComparator();

		@Override
		public int compare(Item o1, Item o2) {
			return cmp.compare(o1.key, o2.key);
		}
	}
}
