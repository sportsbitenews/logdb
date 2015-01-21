package org.araqne.logdb.sort;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Comparator;

import org.junit.Test;

public class MultiRunIteratorTest {
	@Test
	public void jumpTest() throws IOException {
		ParallelMergeSorter sorter = new ParallelMergeSorter(new ItemComparer());
	
		
		for(int i = 200000; i > 0; i--) {
			Item item = new Item(i, i);
			sorter.add(item);
		}
		
		MultiRunIterator it = (MultiRunIterator) sorter.sort();

		System.out.println("statrt test");
		int key1 = 50000;
		it.jump(new Item(key1, 0), new ItemComparer());
		assertEquals((Integer) it.next().getKey(), (Integer) key1);
		
		int key2 = 50001;
		it.jump(new Item(key2, 0), new ItemComparer());
		assertEquals((Integer) it.next().getKey(), (Integer) key2);
	
		int key3 = -5;
		it.reset();
		it.jump(new Item(key3, 0), new ItemComparer());
		assertEquals((Integer) it.next().getKey(), (Integer) 1);
		
		int key4 = 21000;
		it.reset();
		it.jump(new Item(key4, 0), new ItemComparer());
		assertEquals((Integer) it.next().getKey(), (Integer) 21000);
	
		int key5 = 210000;
		it.reset();
		it.jump(new Item(key5, 0), new ItemComparer());
		assertEquals((Integer) it.next().getKey(), (Integer) 200000);
	}
	
	private static class ItemComparer implements Comparator<Item> {

		@Override
		public int compare(Item o1, Item o2) {
			return (Integer) o1.getKey() - (Integer) o2.getKey();
		}
	}
}
