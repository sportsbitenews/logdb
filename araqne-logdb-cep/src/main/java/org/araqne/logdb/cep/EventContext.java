/*
 * Copyright 2014 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb.cep;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.araqne.logdb.Row;

public class EventContext implements EventClockItem {
	private EventKey key;
	private List<Row> rows;

	private AtomicInteger counter = new AtomicInteger();

	// log time or real time
	private long created;

	// 0 means infinite, absolutely disappears
	private long expireTime;

	// 0 means infinite, extended when new row arrives
	private AtomicLong timeoutTime;

	private int maxRows;

	private HashMap<String, Object> variables = new HashMap<String, Object>(1);

	private CopyOnWriteArraySet<EventContextListener> listeners = new CopyOnWriteArraySet<EventContextListener>();

	public EventContext(EventKey key, long created, long expireTime, long timeoutTime, int maxRows) {
		this.key = key;
		this.created = created;
		this.rows = Collections.synchronizedList(new ArrayList<Row>());
		this.expireTime = expireTime;
		this.timeoutTime = new AtomicLong(timeoutTime);
		this.maxRows = maxRows;
	}

	public EventKey getKey() {
		return key;
	}

	public void setKey(EventKey key) {
		this.key = key;
	}

	public List<Row> getRows() {
		return new ArrayList<Row>(rows);
	}

	public long getTimeoutTime() {
		return timeoutTime.get();
	}

	public void setTimeoutTime(long newTimeoutTime) {
		// set only if when timeout time is increased
		while (true) {
			long l = timeoutTime.get();
			if (newTimeoutTime < l)
				break;

			if (timeoutTime.compareAndSet(l, newTimeoutTime)) {
				for (EventContextListener listener : listeners) {
					listener.onUpdateTimeout(this);
				}
				break;
			}
		}
	}

	public long getExpireTime() {
		return expireTime;
	}

	public void setExpireTime(long expireTime) {
		this.expireTime = expireTime;
	}

	public AtomicInteger getCounter() {
		return counter;
	}

	public int getMaxRows() {
		return maxRows;
	}

	@Override
	public String getHost() {
		return key.getHost();
	}

	public long getCreated() {
		return created;
	}

	public void setCreated(long created) {
		this.created = created;
	}

	public void addRow(Row row) {
		synchronized (rows) {
			if (rows.size() < maxRows) {
				// deep copy is required. passed row can be modified later.
				row = new Row(Row.clone(row.map()));
				rows.add(row);
			}
		}
	}

	public void removeRow(Row row) {
		synchronized (rows) {
			rows.remove(row);
		}
	}

	public Map<String, Object> getVariables() {
		return Row.clone(variables);
	}

	@SuppressWarnings("unchecked")
	public Object getVariable(String key) {
		synchronized (variables) {
			Object o = variables.get(key);

			// prevent input data corruption from outside
			if (o instanceof Collection)
				return Row.clone((Collection<Object>) o);
			if (o instanceof Map)
				return Row.clone((Map<String, Object>) o);

			return o;
		}
	}

	@SuppressWarnings("unchecked")
	public void setVariable(String key, Object value) {
		// prevent input data corruption from outside
		if (value instanceof Collection)
			value = Row.clone((Collection<Object>) value);
		if (value instanceof Map)
			value = Row.clone((Map<String, Object>) value);

		synchronized (variables) {
			variables.put(key, value);
		}
	}

	public CopyOnWriteArraySet<EventContextListener> getListeners() {
		return listeners;
	}

	public void setListeners(CopyOnWriteArraySet<EventContextListener> listeners) {
		this.listeners = listeners;
	}

	public EventContext clone() {
		EventContext context =  new EventContext(key, created, expireTime, timeoutTime.get(), maxRows);
		context.counter.set(counter.get());

		for(Row row : rows) 
			context.addRow(row);
		
		for(Entry<String, Object> entry : getVariables().entrySet()) 
			context.setVariable(entry.getKey(), entry.getValue());
		
		return context;
	}
	
	@Override
	public int hashCode() {
		return key.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;

		// must be same class
		if (getClass() != obj.getClass())
			return false;

		// topic and key is always not null
		EventContext other = (EventContext) obj;

		// if (host == null) {
		// if (other.host != null)
		// return false;
		// return key.equals(other.key) && topic.equals(other.topic);
		// }
		
//		if(counter.get() == other.getCounter().get()) {
//			System.out.println("counter equals" + counter.get());
//		} else 
//			System.out.println(counter + " " + other.getCounter());
//		
//		if(variables.equals(other.getVariables()))
//			System.out.println("variables equals " + variables);
		

//		List<Row> otherRows = other.getRows();
//		for(int i = 0; i < rows.size(); i++) {
//			if(rows.get(i).equals(otherRows.get(i))) {
//			} 
//		}
//		if(Arrays.deepEquals(rows.toArray(), other.getRows().toArray()))
//			System.out.println("dd row equals" + rows);
//		
//		
//		if(equalRowLists(rows, other.getRows())) 
//			System.out.println("row equals" + rows);
//		else 
//			System.out.println(rows + "   " + other.getRows());

		return key.equals(other.getKey()) 
				&& created == other.getCreated() 
				&& expireTime == other.getExpireTime()
				&& timeoutTime.get() == other.getTimeoutTime()
				&& maxRows == other.getMaxRows()
				&& counter.get() == other.getCounter().get()
			//	&& rows.equals(other.getRows())
				&& variables.equals(other.getVariables());
	}
	
	public  boolean equalRowLists(List<Row> one, List<Row> two){     
	    if (one == null && two == null){
	        return true;
	    }

	    if((one == null && two != null) 
	      || one != null && two == null
	      || one.size() != two.size()){
	        return false;
	    }

	    //to avoid messing the order of the lists we will use a copy
	    //as noted in comments by A. R. S.
	//    one = new ArrayList<Row>(one); 
	 //   two = new ArrayList<Row>(two);   

	    //Collections.sort(one);
	    //Collections.sort(two);      
	    return one.equals(two);
	}
}
