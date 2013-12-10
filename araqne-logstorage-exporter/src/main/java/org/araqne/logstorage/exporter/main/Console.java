package org.araqne.logstorage.exporter.main;

import java.io.File;
import java.io.IOException;
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
import org.araqne.logstorage.exporter.impl.LogDatFileReaderV2;
import org.araqne.logstorage.exporter.impl.LogDatFileReaderV3;
import org.araqne.logstorage.exporter.impl.LogJsonWriter;
import org.araqne.logstorage.exporter.impl.LogTxtWriter;

public class Console {

	public static void main(String[] args) {
		ConsoleAppender ca = new ConsoleAppender(new PatternLayout());
		ca.setThreshold(Level.INFO);
		org.apache.log4j.BasicConfigurator.configure(ca);

		ExportOption option = getOptions(args);

		Set<File> matchedFiles = findMatchedFiles(new File(option.getFilePath()));

		for (File f : matchedFiles) {
			System.out.println("=======" + f.getAbsolutePath() + "=======\n");
			LogWriter writer = null;
			LogDatFileReader reader = null;
			if (option.getVersion() == 2)
				reader = new LogDatFileReaderV2(f);
			else
				reader = new LogDatFileReaderV3(f, option.getPfxFile(), option.getPassword());

			try {
				File outputFile = getOutputFile(option, f.getName());
				writer = newWriter(outputFile, option);

				long base = System.currentTimeMillis();
				long writeCount = 0;
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
								+ "logs written");
						base = now;
					}
				}
				if (!option.isUseStandardOutput())
					System.out.println("output file: " +
							outputFile.getAbsolutePath() + " write complete. total: " + writeCount);
			} catch (IOException e) {
				System.out.println("cannot create output writer");
			} finally {
				if (reader != null)
					reader.close();
				if (writer != null)
					writer.close();
			}
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

		if (opts.containsKey("-v"))
			option.setVersion(Integer.valueOf(opts.get("-v")));

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
		option.setFilePath(args[args.length - 1]);

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
}
