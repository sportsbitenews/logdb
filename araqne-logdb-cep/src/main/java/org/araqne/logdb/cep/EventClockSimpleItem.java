package org.araqne.logdb.cep;

public class EventClockSimpleItem implements EventClockItem {

	protected EventKey key;
	protected long expireTime;
	protected long timeoutTime;

	public EventClockSimpleItem(EventKey key, long expireTime, long timeoutTime) {
		this.key = key;
		this.expireTime = expireTime;
		this.timeoutTime = timeoutTime;
	}

	@Override
	public EventKey getKey() {
		return key;
	}

	@Override
	public long getExpireTime() {
		return expireTime;
	}

	@Override
	public void setExpireTime(long expireTime) {
		this.expireTime = expireTime;
	}

	@Override
	public long getTimeoutTime() {
		return timeoutTime;
	}

	@Override
	public void setTimeoutTime(long timeoutTime) {
		this.timeoutTime = timeoutTime;
	}

	public static EventClockSimpleItem newInstance(EventClockItem ctx) {
		return new EventClockSimpleItem(ctx.getKey(), ctx.getExpireTime(), ctx.getTimeoutTime());
	}

}
