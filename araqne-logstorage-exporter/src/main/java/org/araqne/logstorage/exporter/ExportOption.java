package org.araqne.logstorage.exporter;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class ExportOption {
	private Set<String> filePaths;
	private String outputType = "txt";
	private List<String> columns = Arrays.asList("line");;
	private File pfxFile;
	private String password;
	private long limitCount = Long.MAX_VALUE;
	private boolean useCompress = false;
	private boolean useStandardOutput = false;
	private File outputDir = new File(System.getProperty("user.dir"));
	private int version = 3;

	public Set<String> getFilePaths() {
		return filePaths;
	}

	public void setFilePaths(Set<String> filePaths) {
		this.filePaths = filePaths;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public String getOutputType() {
		return outputType;
	}

	public void setOutputType(String outputType) {
		this.outputType = outputType;
	}

	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	public File getPfxFile() {
		return pfxFile;
	}

	public void setPfxFile(File pfxFile) {
		this.pfxFile = pfxFile;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public long getLimitCount() {
		return limitCount;
	}

	public void setLimitCount(long limitCount) {
		this.limitCount = limitCount;
	}

	public boolean isUseCompress() {
		return useCompress;
	}

	public void setUseCompress(boolean useCompress) {
		this.useCompress = useCompress;
	}

	public boolean isUseStandardOutput() {
		return useStandardOutput;
	}

	public void setUseStandardOutput(boolean useStandardOutput) {
		this.useStandardOutput = useStandardOutput;
	}

	public File getOutputDir() {
		return outputDir;
	}

	public void setOutputDir(File outputDir) {
		this.outputDir = outputDir;
	}
}
