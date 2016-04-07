package org.araqne.logdb.cep.offheap.allocator;

public class IntegerFirstFitAllocator implements Allocator {

	private AllocableArea<?> storage;
	private final int addressSize;
	private final int minChunkSize;
	private int freeHeadChunk;
	private int freeTailChunk;
	private final int NULL = -1;

	public IntegerFirstFitAllocator(AllocableArea<?> storage) {
		this.storage = storage;
		this.addressSize = 4;
		this.minChunkSize = addressSize * 4;

		initialStorage();
	}

	@Override
	public int allocate(int size) {
		int required = align(size + minChunkSize + 100);
		Chunk free = load(freeHeadChunk);
		free = free.forward();

		while (free.address != freeTailChunk) {
			if (free.size() >= required) {
				// fit
				if (free.size() < required * 2) {
					free.inuse();
					return free.address + minChunkSize;
				}
				// split
				int exAddress = free.address;
				free.split(required);

				return exAddress + minChunkSize;
			}
			free = free.forward();
		}
		return -1;
	}

	private int align(int p) {
		return ((p & 0x01) == 0x01) ? p + 1 : p;
	}

	@Override
	public void free(int p) {
		int address = p - minChunkSize;
		Chunk thisChunk = load(address);
		Chunk prevChunk;
		Chunk nextChunk;

		if ((thisChunk.address - thisChunk.prev_size) != freeHeadChunk && (prevChunk = thisChunk.prev()).free) {
			prevChunk.size(prevChunk.size + thisChunk.size(), false);
			thisChunk = prevChunk;
		}

		if ((thisChunk.address + thisChunk.size < freeTailChunk)
				&& (nextChunk = load(thisChunk.address + thisChunk.size())).free) {
			thisChunk.size(thisChunk.size() + nextChunk.size, false);
			remove(nextChunk);
		}

		if (!thisChunk.free) {
			thisChunk.free(true);
			add(thisChunk);
		}
	}

	@Override
	public void clear() {
		initialStorage();
	}

	public int freeChunk() {
		int i = -1;
		Chunk free = load(freeHeadChunk);
		while (true) {
			if (free.forward == NULL)
				break;

			free = free.forward();
			i++;
		}
		return i;
	}

	@Override
	public int space(int address) {
		Chunk chunk = load(address - minChunkSize);
		return chunk.size();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("(free chunk) : ");
		Chunk free = load(freeHeadChunk);
		while (true) {
			sb.append(free.toString());
			sb.append("----");
			if (free.forward == NULL)
				break;

			free = free.forward();
		}
		sb.append("\n");

		Chunk chunk = load(0);
		sb.append("(memory) : ");

		while (true) {
			sb.append(chunk.toString());
			sb.append("----");
			if ((chunk.address + chunk.size) >= storage.capacity())
				break;

			chunk = chunk.next();
		}
		return sb.toString();
	}

	private void add(Chunk c) {
		c.forward(load(freeHeadChunk).forward());
		c.back(load(freeHeadChunk));
	}

	private void remove(Chunk c) {
		c.forward().back(c.back());
		c.back().forward(c.forward());
	}

	private void initialStorage() {
		Chunk headChunk = new Chunk(0, 0, minChunkSize, -1, minChunkSize);
		Chunk freeChunk = new Chunk(minChunkSize, minChunkSize, storage.capacity() - (2 * minChunkSize), 0,
				storage.capacity() - minChunkSize);
		Chunk tailChunk = new Chunk(storage.capacity() - minChunkSize, storage.capacity() - (2 * minChunkSize),
				minChunkSize, minChunkSize, -1);

		update(headChunk, freeChunk, tailChunk);

		freeHeadChunk = headChunk.address;
		freeTailChunk = tailChunk.address;
	}

	private void update(Chunk... chunks) {
		for (Chunk c : chunks) {
			if (valid(c)) {
				System.out.println(c);
				System.out.println(this);
				throw new IllegalStateException("out of memory");
			}

			if (c.back < -1 || c.forward < -1) {
				System.out.println("update : " + c.address + " ~ " + (c.address + (3 * addressSize)) + " " + c);
				System.out.println("prev" + c.prev());
				System.out.println("next" + c.next());
				System.out.println("forward" + c.forward());
				System.out.println("back" + c.back());

			}

			storage.setAddress(c.address, c.prev_size);
			storage.setAddress(c.address + (1 * addressSize), c.size);
			storage.setAddress(c.address + (2 * addressSize), c.back);
			storage.setAddress(c.address + (3 * addressSize), c.forward);
		}
	}

	private boolean valid(Chunk c) {
		return (c.address > storage.capacity()) || (c.prev_size > storage.capacity()) || (c.size > storage.capacity())
				|| (c.forward > storage.capacity()) || (c.back > storage.capacity());

	}

	private Chunk load(int address) {
		int prev_size = storage.getAddress(address);
		int size = storage.getAddress(address + (1 * addressSize));
		int back = storage.getAddress(address + (2 * addressSize));
		int forward = storage.getAddress(address + (3 * addressSize));

		return new Chunk(address, prev_size, size, back, forward);
	}

	private class Chunk {
		private final static int prevInuseBit = 0x1; // 이전 노드가 사용중인지 확인 비트

		private int back;
		private int forward;
		int prev_size; /* size of previous chunk (if free) */
		int size; /* Size in bytes, including overhead */
		private int address;
		private boolean free;

		private Chunk(int address, int prev_size, int size, int back, int forward) {
			this.address = address;
			this.prev_size = prev_size;
			this.size = size;
			this.back = back;
			this.forward = forward;
			this.free = (size & prevInuseBit) != prevInuseBit;
		}

		public Chunk split(int required) {
			Chunk freeNode = new Chunk(address + required, required, size - required, back, forward);
			update(freeNode);

			freeNode.back(this.back());
			freeNode.forward(this.forward());
			freeNode.size(size - required, false);
			size = required;
			free(false);

			return freeNode;
		}

		public void inuse() {
			free(false);
			remove(this);
		}

		public int size() {
			return size & ~prevInuseBit;
		}

		public void size(int size, boolean inuse) {
			this.size = inuse ? size | prevInuseBit : size;
			int nextAddress = address + size;
			if (nextAddress < storage.capacity()) {
				Chunk next = load(nextAddress);
				next.prev_size = this.size;
				update(this, next);
			} else {
				update(this);
			}
		}

		public Chunk prev() {
			return load(address - prev_size);
		}

		public Chunk next() {
			return load(address + size());
		}

		public void forward(Chunk f) {
			this.forward = f.address;
			f.back = address;
			update(this, f);
		}

		public Chunk forward() {
			return load(forward);
		}

		public void back(Chunk b) {
			this.back = b.address;
			b.forward = this.address;
			update(b, this);
		}

		public Chunk back() {
			return load(back);
		}

		public void free(boolean b) {
			free = b;
			if (b) {
				size &= ~prevInuseBit;
			} else {
				size |= prevInuseBit;
			}

			update(this);
		}

		@Override
		public String toString() {
			return "[*" + address + "][" + prev_size + "|" + size + "(" + size() + free + ")|<-" + back + "|" + forward
					+ "->]";
		}
	}
}
