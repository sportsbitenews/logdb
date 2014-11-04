package org.araqne.log.api;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class StringAnon {
	public static class Mask {
		public long mask;
		public long pad;

		public Mask(long mask, long pad) {
			this.mask = mask;
			this.pad = pad;
		}
	}

	private Cipher cipher;
	private Mask[] masks;
	private String aesKey;
	private byte[] pad;

	public StringAnon(String key) {
		if (key.length() != 32) {
			throw new IllegalArgumentException("key must me a 32 byte long string");
		}

		this.aesKey = key.substring(0, 16);

		SecretKeySpec keyspec = new SecretKeySpec(aesKey.getBytes(), "AES");
		try {
			cipher = javax.crypto.Cipher.getInstance("AES/CBC/NoPadding");
			cipher.init(javax.crypto.Cipher.ENCRYPT_MODE, keyspec, new IvParameterSpec(new byte[16]));
			this.pad = cipher.doFinal(key.substring(16).getBytes());
			byte[] f4 = Arrays.copyOf(this.pad, 4);
			long f4bp = toInt(f4);

			this.masks = new Mask[32];
			for (int p = 0; p < 32; ++p) {
				long mask = 0xFFFFFFFFL >> (32 - p) << (32 - p);
				masks[p] = new Mask(mask, f4bp & (~mask));
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (NoSuchPaddingException e) {
			e.printStackTrace();
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		} catch (InvalidAlgorithmParameterException e) {
			e.printStackTrace();
		} catch (IllegalBlockSizeException e) {
			e.printStackTrace();
		} catch (BadPaddingException e) {
			e.printStackTrace();
		}

	}

	private long toInt(byte[] arr) {
		long l0 = ((long) arr[0] & 0xffL) << (8 * (3 - 0));
		long l1 = ((long) arr[1] & 0xffL) << (8 * (3 - 1));
		long l2 = ((long) arr[2] & 0xffL) << (8 * (3 - 2));
		long l3 = ((long) arr[3] & 0xffL) << (8 * (3 - 3));

		return l0 | l1 | l2 | l3;
	}

	public void reset() throws IllegalBlockSizeException, BadPaddingException {
		cipher.doFinal();
	}

	public byte[] anonymize2(byte[] b4) throws IllegalBlockSizeException, BadPaddingException {
		do {
			pad[0] = (byte) (b4[0] & 0xFF);
			if (b4.length == 1)
				break;
			pad[1] = (byte) (b4[1] & 0xFF);
			if (b4.length == 2)
				break;
			pad[2] = (byte) (b4[2] & 0xFF);
			if (b4.length == 3)
				break;
			pad[3] = (byte) (b4[3] & 0xFF);
		} while (false);
		byte[] doFinal = cipher.update(pad);

		return Arrays.copyOfRange(doFinal, 0, 4);
	}

	public byte[] anonymize(byte[] b4) {
		long result = 0;
		byte[] addria = b4;
		long addri = toInt(addria);

		long[] addresses = new long[32];
		for (int i = 0; i < this.masks.length; ++i) {
			addresses[i] = (addri & masks[i].mask) | masks[i].pad;
		}
		int[] calcResult = new int[32];
		try {
			for (int i = 0; i < 32; ++i) {
				calcResult[i] = calc(addresses[i]);
			}

			result = 0;
			for (int i = 0; i < 32; ++i) {
				result = (result << 1) | calcResult[i];
			}

			byte[] rarr = toArray(result ^ addri);
			return rarr;
		} catch (IllegalBlockSizeException e) {
			e.printStackTrace();
		} catch (BadPaddingException e) {
			e.printStackTrace();
		}
		throw new IllegalStateException();
	}

	private byte[] toArray(long n) {
		byte[] result = new byte[4];
		for (int i = 3; i > -1; --i) {
			result[3 - i] = (byte) (n >> (i * 8) & 0xFF);
		}
		return result;
	}

	// calculate the first bit for Crypto-PAN
	private int calc(long a) throws IllegalBlockSizeException, BadPaddingException {
		pad[3] = (byte) (a & 0xFF);
		a >>= 8;
		pad[2] = (byte) (a & 0xFF);
		a >>= 8;
		pad[1] = (byte) (a & 0xFF);
		a >>= 8;
		pad[0] = (byte) (a & 0xFF);

		byte[] doFinal = cipher.doFinal(pad);

		return doFinal[0] < 0 ? 1 : 0;
	}

	public static void main(String[] args) throws InterruptedException {
		StringBuilder sb = new StringBuilder();
		int k = 5;
		for (int i = k; i < k + 32; ++i) {
			sb.append((char) i);
		}
		StringAnon c = new StringAnon(sb.toString());
		System.out.println(c.anonymize2("stania", "utf-8", Options.PreserveNumber));
		System.out.println(c.anonymize2("stania", "utf-8", Options.PreserveNumber));
		System.out.println(c.anonymize2("stalia", "utf-8", Options.PreserveNumber));
		System.out.println(c.anonymize2("stalia", "utf-8", Options.PreserveNumber));
		System.out.println(c.anonymize2("everclear", "utf-8", Options.PreserveNumber));
		System.out.println(c.anonymize2("evercIear", "utf-8", Options.PreserveNumber));
		System.out.println(c.anonymize2("darkluster", "utf-8", Options.PreserveNumber));
		System.out.println(c.anonymize2("blue1273", "utf-8", Options.PreserveNumber));
		System.out.println(c.anonymize2("jungi2", "utf-8", Options.PreserveNumber));
		System.out.println(c.anonymize("ibicegbukbo", "utf-8", Options.ConsonantOnly));
		System.out.println(c.anonymize("sbiukgbcgbs", "utf-8", Options.ConsonantOnly));

		long started = System.currentTimeMillis();
		for (int i = 0; i < 50000; ++i) {
			c.anonymize("sumbimusbmbcuc");
		}
		System.out.println("elapsed: " + (System.currentTimeMillis() - started));
	}

	private String anonymize(String src) {
		return anonymize(src, "utf-8", Options.PreserveNumber);
	}

	byte[] ba = new byte[4];

	public static enum Options {
		Random,
		ConsonantOnly,
		PreserveNumber,
		HexOnly,
	}

	public String anonymize(String src, String encoding, Options opt) {
		try {
			byte[] bytes = src.getBytes(encoding);
			ba[0] = ba[1] = ba[2] = ba[3] = 0;
			int[] ccnt = new int[] { 0 };
			for (int c = 0; c < bytes.length; c += 4) {
				int r = bytes.length - c;
				do {
					ba[0] = bytes[c + 0];
					if (r == 1)
						break;
					ba[1] = bytes[c + 1];
					if (r == 2)
						break;
					ba[2] = bytes[c + 2];
					if (r == 3)
						break;
					ba[3] = bytes[c + 3];
				} while (false);
				byte[] anon = anonymize(ba);
				updateResult(opt, bytes, ccnt, c, r, anon);
			}

			reset();
			return Charset.forName(encoding).decode(ByteBuffer.wrap(bytes)).toString();
		} catch (UnsupportedEncodingException e) {
			throw new IllegalStateException(e);
		} catch (IllegalBlockSizeException e) {
			throw new IllegalStateException(e);
		} catch (BadPaddingException e) {
			throw new IllegalStateException(e);
		}
	}

	private void updateResult(Options opt, byte[] bytes, int[] ccnt, int c, int r, byte[] result) {
		if (opt == Options.ConsonantOnly) {
			do {
				bytes[c + 0] = selConsonant(result[0]);
				if (r == 1)
					break;
				bytes[c + 1] = selConsonant(result[1]);
				if (r == 2)
					break;
				bytes[c + 2] = selConsonant(result[2]);
				if (r == 3)
					break;
				bytes[c + 3] = selConsonant(result[3]);
			} while (false);
		} else if (opt == Options.PreserveNumber) {
			do {
				bytes[c + 0] = selPreservingNumber(bytes[c + 0], result[0], ccnt);
				if (r == 1)
					break;
				bytes[c + 1] = selPreservingNumber(bytes[c + 1], result[1], ccnt);
				if (r == 2)
					break;
				bytes[c + 2] = selPreservingNumber(bytes[c + 2], result[2], ccnt);
				if (r == 3)
					break;
				bytes[c + 3] = selPreservingNumber(bytes[c + 3], result[3], ccnt);
			} while (false);
		} else if (opt == Options.HexOnly) {
			do {
				bytes[c + 0] = selHexChar(result[0]);
				if (r == 1)
					break;
				bytes[c + 1] = selHexChar(result[1]);
				if (r == 2)
					break;
				bytes[c + 2] = selHexChar(result[2]);
				if (r == 3)
					break;
				bytes[c + 3] = selHexChar(result[3]);
			} while (false);
		} else {
			do {
				bytes[c + 0] = selRandomChar(result[0]);
				if (r == 1)
					break;
				bytes[c + 1] = selRandomChar(result[1]);
				if (r == 2)
					break;
				bytes[c + 2] = selRandomChar(result[2]);
				if (r == 3)
					break;
				bytes[c + 3] = selRandomChar(result[3]);
			} while (false);
		}
	}

	public String anonymize2(String src, String encoding, Options opt) {
		try {
			byte[] bytes = src.getBytes(encoding);
			ba[0] = ba[1] = ba[2] = ba[3] = 0;
			int[] ccnt = new int[] { 0 };
			for (int c = 0; c < bytes.length; c += 4) {
				int r = bytes.length - c;
				byte[] result = anonymize2(Arrays.copyOfRange(bytes, c, c + 4));
				updateResult(opt, bytes, ccnt, c, r, result);
			}

			reset();
			return Charset.forName(encoding).decode(ByteBuffer.wrap(bytes)).toString();
		} catch (UnsupportedEncodingException e) {
			throw new IllegalStateException(e);
		} catch (IllegalBlockSizeException e) {
			throw new IllegalStateException(e);
		} catch (BadPaddingException e) {
			throw new IllegalStateException(e);
		}
	}

	private byte selPreservingNumber(byte s, byte b, int[] ccnt) {
		if (isDigit(s)) {
			ccnt[0] = 0;
			return dictn[(b & 0xff) % dictn.length];
		} else {
			return selReadableChar(b, ccnt);
		}
	}

	private byte selReadableChar(byte b, int[] ccnt) {
		if (ccnt[0]++ % 2 == 0)
			return dictc[(b & 0xff) % dictc.length];
		else
			return dictv[(b & 0xff) % dictv.length];
	}
	
	private byte selConsonant(byte b) {
		return dictc[(b & 0xff) % dictc.length];
	}

	private byte selRandomChar(byte b) {
		return dicta[(b & 0xff) % dicta.length];
	}

	private byte selHexChar(byte b) {
		return dicth[(b & 0xff) % dicth.length];
	}

	private boolean isDigit(byte b) {
		return b >= '0' && b <= '9';
	}

	static byte[] dicta = new byte[] {
			'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
			'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '2', '3', '4', '5', '6', '7' };
	static byte[] dicth = new byte[] {
			'a', 'b', 'c', 'd', 'e', 'f', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
	static byte[] dictc = new byte[] {
			'b', 'c', 'd', 'f', 'g', 'h', 'j', 'k', 'n', 'p', 's', 'r', 't', 'v', 'w', 'x', 'y', 'z' };
	static byte[] dictv = new byte[] {
			'a', 'i', 'u', 'o', 'e', 'y', 's' };
	static byte[] dictn = new byte[] {
			'2', '1', '9', '7', '5', '3', '0', '6', '4', '8', '3', '0', '6', '1', '5', '2', '4', '8', '7', '9' };
}
