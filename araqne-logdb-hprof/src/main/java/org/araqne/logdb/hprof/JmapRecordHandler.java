package org.araqne.logdb.hprof;

import java.util.HashMap;
import java.util.Map;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowPipe;

import edu.tufts.eaftan.hprofparser.handler.RecordHandler;
import edu.tufts.eaftan.hprofparser.parser.HprofParserException;
import edu.tufts.eaftan.hprofparser.parser.datastructures.AllocSite;
import edu.tufts.eaftan.hprofparser.parser.datastructures.CPUSample;
import edu.tufts.eaftan.hprofparser.parser.datastructures.Constant;
import edu.tufts.eaftan.hprofparser.parser.datastructures.InstanceField;
import edu.tufts.eaftan.hprofparser.parser.datastructures.Static;
import edu.tufts.eaftan.hprofparser.parser.datastructures.Type;
import edu.tufts.eaftan.hprofparser.parser.datastructures.Value;

public class JmapRecordHandler implements RecordHandler {
	private Map<Long, String> strings = new HashMap<Long, String>();
	private Map<Long, String> classNames = new HashMap<Long, String>();
	private QueryCommand output;

	public JmapRecordHandler(QueryCommand output) {
		this.output = output;
	}

	@Override
	public void header(String format, int idSize, long time) {
	}

	@Override
	public void stringInUTF8(long id, String data) {
		strings.put(id, data);
	}

	@Override
	public void loadClass(int classSerialNum, long classObjId, int stackTraceSerialNum, long classNameStringId) {
		String className = strings.get(classNameStringId);
		classNames.put(classObjId, className);
	}

	@Override
	public void unloadClass(int classSerialNum) {
	}

	@Override
	public void stackFrame(long stackFrameId, long methodNameStringId, long methodSigStringId, long sourceFileNameStringId,
			int classSerialNum, int location) {
	}

	@Override
	public void stackTrace(int stackTraceSerialNum, int threadSerialNum, int numFrames, long[] stackFrameIds) {
	}

	@Override
	public void allocSites(short bitMaskFlags, float cutoffRatio, int totalLiveBytes, int totalLiveInstances,
			long totalBytesAllocated, long totalInstancesAllocated, AllocSite[] sites) {
	}

	@Override
	public void heapSummary(int totalLiveBytes, int totalLiveInstances, long totalBytesAllocated, long totalInstancesAllocated) {
	}

	@Override
	public void startThread(int threadSerialNum, long threadObjectId, int stackTraceSerialNum, long threadNameStringId,
			long threadGroupNameId, long threadParentGroupNameId) {
	}

	@Override
	public void endThread(int threadSerialNum) {
	}

	@Override
	public void heapDump() {
	}

	@Override
	public void heapDumpEnd() {
	}

	@Override
	public void heapDumpSegment() {
	}

	@Override
	public void cpuSamples(int totalNumOfSamples, CPUSample[] samples) {
	}

	@Override
	public void controlSettings(int bitMaskFlags, short stackTraceDepth) {
	}

	@Override
	public void rootUnknown(long objId) {
	}

	@Override
	public void rootJNIGlobal(long objId, long JNIGlobalRefId) {
	}

	@Override
	public void rootJNILocal(long objId, int threadSerialNum, int frameNum) {
	}

	@Override
	public void rootJavaFrame(long objId, int threadSerialNum, int frameNum) {
	}

	@Override
	public void rootNativeStack(long objId, int threadSerialNum) {
	}

	@Override
	public void rootStickyClass(long objId) {
	}

	@Override
	public void rootThreadBlock(long objId, int threadSerialNum) {
	}

	@Override
	public void rootMonitorUsed(long objId) {
	}

	@Override
	public void rootThreadObj(long objId, int threadSerialNum, int stackTraceSerialNum) {
	}

	@Override
	public void classDump(long classObjId, int stackTraceSerialNum, long superClassObjId, long classLoaderObjId,
			long signersObjId, long protectionDomainObjId, long reserved1, long reserved2, int instanceSize,
			Constant[] constants, Static[] statics, InstanceField[] instanceFields) {
	}

	@Override
	public void instanceDump(long objId, int stackTraceSerialNum, long classObjId, Value<?>[] instanceFieldValues) {
		long bytes = 0;
		for (Value<?> v : instanceFieldValues)
			bytes += v.type.sizeInBytes();

		String className = classNames.get(classObjId);
		Row row = new Row();
		row.put("obj_id", objId);
		row.put("class", className);
		row.put("bytes", bytes);

		output.onPush(row);
	}

	@Override
	public void objArrayDump(long objId, int stackTraceSerialNum, long elemClassObjId, long[] elems) {
		String className = classNames.get(elemClassObjId);
		Row row = new Row();
		row.put("obj_id", objId);
		row.put("class", className);
		row.put("bytes", 8 * elems.length);

		output.onPush(row);
	}

	@Override
	public void primArrayDump(long objId, int stackTraceSerialNum, byte elemType, Value<?>[] elems) {
		Type type = Type.hprofTypeToEnum(elemType);
		String typeName = null;
		switch (elemType) {
		case 2:
			typeName = "Object[]";
			break;
		case 4:
			typeName = "boolean[]";
			break;
		case 5:
			typeName = "char[]";
			break;
		case 6:
			typeName = "float[]";
			break;
		case 7:
			typeName = "double[]";
			break;
		case 8:
			typeName = "byte[]";
			break;
		case 9:
			typeName = "short[]";
			break;
		case 10:
			typeName = "int[]";
			break;
		case 11:
			typeName = "long[]";
			break;
		default:
			return;
		}

		int length = elems != null ? elems.length : 0;

		Row row = new Row();
		row.put("obj_id", objId);
		row.put("class", typeName);
		row.put("bytes", length * type.sizeInBytes());

		output.onPush(row);
	}

	@Override
	public void finished() {
	}

}
