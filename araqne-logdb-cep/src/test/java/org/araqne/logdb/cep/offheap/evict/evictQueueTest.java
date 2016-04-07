package org.araqne.logdb.cep.offheap.evict;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.araqne.logdb.cep.offheap.evict.EvictItem;
import org.araqne.logdb.cep.offheap.evict.EvictQueue;
import org.junit.Test;

public class evictQueueTest {

	/*
	 * test method : add(), size(), remove()
	 */
	@Test
	public void queueTest() {

		EvictQueue queue = new EvictQueue();
		List<EvictItem> items = new ArrayList<EvictItem>();
		List<EvictItem> items2 = new ArrayList<EvictItem>();
		int cnt = 10000;

		for (int i = 0; i < cnt; i++) {
			EvictItem item = new EvictItem(new Random().nextLong(), new Random().nextLong());
			queue.add(item);
			items.add(item);
		}

		assertEquals(items.size(), queue.size());

		for (int i = 0; i < cnt; i++) {
			items2.add(queue.remove());
		}
		Collections.sort(items);

		for (int i = 0; i < cnt; i++) {
			assertEquals(items.get(i).getTime(), items.get(i).getTime());
		}

		queue.close();
	}

}
