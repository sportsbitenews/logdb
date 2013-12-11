package org.araqne.logstorage.exporter.main;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.araqne.logstorage.exporter.ExportOption;
import org.araqne.logstorage.exporter.api.LogDatFileReader;
import org.araqne.logstorage.exporter.api.LogWriter;
import org.araqne.logstorage.exporter.impl.FileWildcardMatcher;
import org.araqne.logstorage.exporter.impl.LogCsvWriter;
import org.araqne.logstorage.exporter.impl.LogDatFileReaderImpl;
import org.araqne.logstorage.exporter.impl.LogJsonWriter;
import org.araqne.logstorage.exporter.impl.LogTxtWriter;

public class Console {

	public static void main(String[] args) {
		ConsoleAppender ca = new ConsoleAppender(new PatternLayout());
		ca.setThreshold(Level.INFO);
		org.apache.log4j.BasicConfigurator.configure(ca);

		ExportOption option = getOptions(args);
		Set<File> matchedFiles = new HashSet<File>();
		for (String filePath : option.getFilePaths()) {
			matchedFiles.addAll(findMatchedFiles(new File(filePath)));
		}

		for (File f : matchedFiles) {
			System.out.println("=======" + f.getAbsolutePath() + "=======");
			LogWriter writer = null;
			LogDatFileReader reader = null;
			long writeCount = 0;
			File outputFile = getOutputFile(option, f.getName());
			if (outputFile.exists()) {
				System.out.println("output file already exists, path: " + outputFile.getAbsolutePath());
				continue;
			}
			try {
				reader = new LogDatFileReaderImpl(f, option.getPfxFile(), option.getPassword());
				writer = newWriter(outputFile, option);

				long base = System.currentTimeMillis();
				boolean isEnd = false;
				while (!isEnd && reader.hasNext()) {
					long now = System.currentTimeMillis();
					List<Map<String, Object>> logs = reader.nextBlock();
					if (logs == null)
						break;
					for (Map<String, Object> log : logs) {
						writer.write(log);
						writeCount++;
						if (writeCount == option.getLimitCount()) {
							isEnd = true;
							break;
						}
					}
					if (!option.isUseStandardOutput() && now - base > 1000) {
						System.out.println("input: " + f.getName() +
								", output: " + outputFile.getName() + ", " + writeCount
								+ " logs written");
						base = now;
					}
				}

			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e.getMessage());
			} finally {
				if (reader != null)
					reader.close();
				if (writer != null)
					writer.close();
			}
			System.out.println("output count: " + formatNumber(writeCount));
			if (!option.isUseStandardOutput())
				System.out.println("output file path: " + outputFile.getAbsolutePath());
			System.out.println("=================end================\n");
		}
	}

	private static ExportOption getOptions(String[] args) {
		if (args.length < 1) {
			System.out.println("cannot find target file");
			throw new IllegalArgumentException("invalid argument");
		}

		Map<String, String> opts = getOpts(args);
		ExportOption option = new ExportOption();

		if (opts.containsKey("-F"))
			option.setOutputType(opts.get("-F"));
		String outputType = option.getOutputType();

		if (!(outputType.equalsIgnoreCase("txt") || outputType.equalsIgnoreCase("json") || outputType.equalsIgnoreCase("csv")))
			throw new IllegalArgumentException("invalid output type");

		if (opts.containsKey("-c")) {
			List<String> columns = new ArrayList<String>();
			String[] split = opts.get("-c").split(",");
			columns = new ArrayList<String>();
			for (int index = 0; index < split.length; index++)
				columns.add(split[index].trim());
			option.setColumns(columns);
		}

		if (opts.containsKey("-k")) {
			String s = opts.get("-k");
			int comma = s.indexOf(",");

			if (comma == -1)
				throw new IllegalArgumentException("invalid pfx parameter");
			String pfxPath = s.substring(0, comma);
			File pfxFile = new File(pfxPath);
			if (!pfxFile.exists())
				throw new IllegalStateException("not-found-pfx-file");

			option.setPfxFile(pfxFile);
			option.setPassword(s.substring(comma + 1));
		}

		if (opts.containsKey("-l"))
			option.setLimitCount(Long.valueOf(opts.get("-l")));

		option.setUseStandardOutput(opts.containsKey("-O"));
		option.setUseCompress(opts.containsKey("-z"));

		if (opts.containsKey("-d")) {
			String outputPath = opts.get("-d");
			File outputDir = new File(outputPath);
			if (!outputDir.exists() || !outputDir.isDirectory())
				outputDir.mkdir();
			option.setOutputDir(outputDir);
		} else
			option.setOutputDir(new File(System.getProperty("user.dir")));
		Set<String> fileNames = new HashSet<String>();
		for (int index = opts.keySet().size(); index < args.length; index++) {
			String fileName = args[index];
			if (fileName.endsWith("dat"))
				fileNames.add(fileName);
		}
		option.setFilePaths(fileNames);

		return option;
	}

	public static File getOutputFile(ExportOption option, String inputFileName) {
		String outputFileName = inputFileName.substring(0, inputFileName.lastIndexOf(".")) + "." + option.getOutputType();
		if (!option.isUseStandardOutput() && option.isUseCompress())
			outputFileName += ".gz";
		return new File(option.getOutputDir(), outputFileName);
	}

	private static LogWriter newWriter(File outputFile, ExportOption option)
			throws IOException {
		String outputType = option.getOutputType();
		if (outputType.equalsIgnoreCase("txt"))
			return new LogTxtWriter(outputFile, option);
		else if (outputType.equalsIgnoreCase("json"))
			return new LogJsonWriter(outputFile, option);
		else if (outputType.equalsIgnoreCase("csv"))
			return new LogCsvWriter(outputFile, option);
		else
			throw new UnsupportedOperationException("invalid output type");
	}

	private static Set<File> findMatchedFiles(File baseFile) {
		Set<String> fileNames = new HashSet<String>();
		File parentFile = baseFile.getParentFile();
		if (parentFile == null)
			parentFile = new File(System.getProperty("user.dir"));
		for (File file : parentFile.listFiles()) {
			if (file.getName().endsWith("dat"))
				fileNames.add(file.getName());
		}

		Set<File> targetFiles = new HashSet<File>();
		for (String fileName : FileWildcardMatcher.apply(fileNames, baseFile.getName())) {
			targetFiles.add(new File(parentFile, fileName));
		}

		return targetFiles;
	}

	private static Map<String, String> getOpts(String[] args) {
		String name = null;
		String value = "";

		Map<String, String> opts = new HashMap<String, String>();
		for (String arg : args) {
			if (arg.startsWith("-")) {
				if (name != null) {
					opts.put(name, value);
					value = "";
				}

				name = arg;
			} else {
				value = arg;
				if (name != null) {
					opts.put(name, value);
					name = null;
				}
			}
		}
		return opts;
	}

	private static String formatNumber(long writeCount) {
		DecimalFormat formatter = new DecimalFormat("###,###");
		return formatter.format(writeCount);
	}
}
