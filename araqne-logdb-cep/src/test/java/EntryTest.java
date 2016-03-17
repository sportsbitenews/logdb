import org.araqne.logdb.cep.offheap.engine.Entry;
import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;
import org.junit.Test;

public class EntryTest {

	@Test
	public void encodeTest() {
		Entry<String, String> entry = new Entry<String, String>("encodeKey", "encodeValue", 1, 2, 3, 4);
		byte[] encoded = Entry.encode(entry, Serialize.STRING, Serialize.STRING);
//		System.out.println(Entry.sizeOf(Serialize.STRING.serialize("encodeKey"),
//				Serialize.STRING.serialize("encodeValue").length));
//		System.out.println(encoded.length);
//		Entry<String, String> newEntry = Entry.decode(encoded, Serialize.STRING, Serialize.STRING, 4);
//		System.out.println(newEntry);
	}
}
