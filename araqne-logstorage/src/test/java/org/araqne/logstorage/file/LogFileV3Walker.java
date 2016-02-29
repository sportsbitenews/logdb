package org.araqne.logstorage.file;

import java.io.IOException;
import java.security.PrivateKey;
import java.security.PublicKey;

import org.araqne.logstorage.file.LogFileV3Reader.LogBlockV3;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.crypto.BlockCipher;
import org.araqne.storage.crypto.LogCryptoException;
import org.araqne.storage.crypto.LogCryptoService;
import org.araqne.storage.crypto.MacBuilder;
import org.araqne.storage.crypto.PkiCipher;
import org.araqne.storage.localfile.LocalFilePath;

public class LogFileV3Walker {
	public static void main(String[] args) throws IOException, LogCryptoException {
		FilePath indexPath = new LocalFilePath(args[0]);
		FilePath dataPath = indexPath.getAbsoluteFilePath().getParentFilePath()
				.newFilePath(indexPath.getName().replace(".idx", ".dat"));
		LogFileV3Reader reader = new LogFileV3Reader(indexPath, dataPath, null, null, new LogCryptoService() {

			@Override
			public PkiCipher newPkiCipher(PublicKey publicKey, PrivateKey privateKey) throws LogCryptoException {
				return null;
			}

			@Override
			public PkiCipher newPkiCipher(PublicKey publicKey) throws LogCryptoException {
				return null;
			}

			@Override
			public MacBuilder newMacBuilder(String algorithm, byte[] digestKey) throws LogCryptoException {
				return null;
			}

			@Override
			public BlockCipher newBlockCipher(String algorithm, byte[] cipherKey) throws LogCryptoException {
				return null;
			}
		});

		System.out.printf("%s blocks\n", reader.getBlockCount());
		while (reader.hasNextBlock()) {
			LogBlockV3 block = reader.nextBlock();
			System.out.printf("%s [%s ~ %s] %s%s: %s - %s\n", block.getIndexBlock().getId(), block.getMinId(), block.getMaxId(),
					block.getDataFp(), (block.getIndexBlock().isReserved() ? " reserved" : ""), block.getCompressedSize(),
					block.getDataHash());
		}

		reader.close();
	}

}
