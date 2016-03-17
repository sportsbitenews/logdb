package org.araqne.logdb.cep.offheap.timeout;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

public class TimeoutQueueTest {

	@Test
	public void test() {

		TimeoutQueue queue = new TimeoutQueue();
		List<TimeoutItem> items = new ArrayList<TimeoutItem>();
		List<TimeoutItem> items2 = new ArrayList<TimeoutItem>();
		int cnt = 10000;
		
		for (int i = 0; i < cnt; i++) {
			TimeoutItem item = new TimeoutItem(new Random().nextLong(), new Random().nextLong());
			queue.add(item);
			items.add(item);
		}

		for (int i = 0; i < cnt; i++) {
			items2.add(queue.remove());
		}
		Collections.sort(items);

		
		for (int i = 0; i < cnt; i++) {
			//System.out.println(items.get(i).getTime() + " " +  items.get(i).getTime());
			assertEquals(items.get(i).getTime(), items.get(i).getTime());
		}

		queue.close();
	}

}
