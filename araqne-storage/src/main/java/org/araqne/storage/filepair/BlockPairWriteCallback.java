package org.araqne.storage.filepair;

public interface BlockPairWriteCallback {
	void onWritingHeader(BlockPairWriteCallbackArgs arg);
	void onWriteCompleted(BlockPairWriteCallbackArgs arg);
}