package org.araqne.logstorage.file;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.confdb.ConfigService;
import org.araqne.log.api.TimeZoneMappings;
import org.araqne.logstorage.CallbackSet;
import org.araqne.logstorage.LogFileService;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.StorageConfig;
import org.araqne.logstorage.TableConfig;
import org.araqne.logstorage.TableConfigSpec;
import org.araqne.logstorage.TableSchema;
import org.araqne.logstorage.engine.ConfigUtil;
import org.araqne.logstorage.engine.Constants;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.FilePathNameFilter;
import org.araqne.storage.api.StorageManager;
import org.araqne.storage.localfile.LocalFilePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(name = "logstorage-log-file-service-txt", immediate = true)
public class LogFileServiceTxt implements LogFileService {
	private final Logger logger = LoggerFactory.getLogger(LogFileServiceV2.class);

	private static final String OPT_STORAGE_CONFIG = "storageConfig";
	private static final String OPT_TABLE_NAME = "tableName";
	private static final String OPT_DAY = "day";
	private static final String OPT_BASE_PATH = "basePath";
	private static final String OPT_INDEX_PATH = "indexPath";
	private static final String OPT_DATA_PATH = "dataPath";
	private static final String OPT_KEY_PATH = "keyPath";
	private static final Object OPT_CALLBACK_SET = "callbackSet";
	private static final Object OPT_LASTKEY = "lastKey";

	private FilePath logDir;

	public static class Option extends TreeMap<String, Object> {
		private static final long serialVersionUID = 1L;

		public Option(StorageConfig config, Map<String, String> tableMetadata, String tableName, FilePath basePath,
				FilePath indexPath, FilePath dataPath, FilePath keyPath) {
			this.put(OPT_STORAGE_CONFIG, config);
			this.putAll(tableMetadata);
			this.put(OPT_TABLE_NAME, tableName);
			this.put(OPT_BASE_PATH, basePath);
			this.put(OPT_INDEX_PATH, indexPath);
			this.put(OPT_DATA_PATH, dataPath);
			this.put(OPT_KEY_PATH, keyPath);
		}
	}

	@Requires
	private LogTableRegistry tableRegistry;

	@Requires
	private LogFileServiceRegistry registry;

	@Requires
	private StorageManager storageManager;

	@Requires
	private ConfigService conf;

	@Validate
	public void start() {
		logDir = storageManager.resolveFilePath(System.getProperty("araqne.data.dir")).newFilePath("araqne-logstorage/log");
		logDir = storageManager.resolveFilePath(getStringParameter(Constants.LogStorageDirectory, logDir.getAbsolutePath()));
		logDir.mkdirs();

		registry.register(this);
	}

	private String getStringParameter(Constants key, String defaultValue) {
		String value = ConfigUtil.get(conf, key);
		if (value != null)
			return value;
		return defaultValue;
	}

	@Invalidate
	public void stop() {
		if (registry != null)
			registry.unregister(this);
	}

	@Override
	public String getType() {
		return "txt";
	}

	@Override
	public long count(FilePath f) {
		return 0;
	}

	private String retrieveConfig(StorageConfig primaryStorage, String configKey, String defaultValue) {
		String configValue = defaultValue;
		TableConfig config = primaryStorage.getConfig(configKey);
		if (config != null)
			configValue = config.getValue();

		return configValue;
	}

	private String convertToRegex(String target) {
		String regexStr = target;

		if (target != null && !target.isEmpty()) {
			regexStr = regexStr.replace(".", "\\.");
			regexStr = regexStr.replace("*", ".*");
			regexStr = regexStr.replace("?", ".?");
		}
		return regexStr;
	}

	@Override
	public List<Date> getPartitions(String tableName) {

		TableSchema schema = tableRegistry.getTableSchema(tableName, true);
		StorageConfig primaryStorage = schema.getPrimaryStorage();

		String fileNamePrefix = retrieveConfig(primaryStorage, "filename_prefix", "");
		String dateFormatString = retrieveConfig(primaryStorage, "date_format", "");
		String fileNameSuffix = retrieveConfig(primaryStorage, "filename_suffix", "");

		fileNameSuffix = convertToRegex(fileNameSuffix);

		String dateLocale = retrieveConfig(primaryStorage, "date_locale", "en");
		SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatString, new Locale(dateLocale));

		String timeZone = retrieveConfig(primaryStorage, "timezone", "");
		if (!timeZone.isEmpty()) {
			if (TimeZoneMappings.getTimeZone(timeZone) != null)
				timeZone = (String) TimeZoneMappings.getTimeZone(timeZone);
			dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
		}

		FilePath baseDir = logDir.newFilePath(Integer.toString(schema.getId()));
		if (schema.getPrimaryStorage().getBasePath() != null)
			baseDir = storageManager.resolveFilePath(schema.getPrimaryStorage().getBasePath());

		FilePath[] filesFilteredByPrefix = filterByPrefix(baseDir, fileNamePrefix);

		String[] dateFormatsSplitedBySlash = dateFormatString.split("/");
		SimpleDateFormat[] splitedDateFormatArr = new SimpleDateFormat[dateFormatsSplitedBySlash.length];
		for (int i = 0; i < dateFormatsSplitedBySlash.length; i++) {
			splitedDateFormatArr[i] = new SimpleDateFormat(dateFormatsSplitedBySlash[i], new Locale(dateLocale));
			if (!timeZone.isEmpty())
				splitedDateFormatArr[i].setTimeZone(TimeZone.getTimeZone(timeZone));
		}
		int reflectedPathCharCnt = baseDir.getAbsolutePath().length() + fileNamePrefix.length();
		boolean isDateFormatEndWithSlash = dateFormatString.endsWith("/");

		List<FilePath> filesFilteredByDateFormat = filterByDateFormat(filesFilteredByPrefix, splitedDateFormatArr, 0,
				reflectedPathCharCnt, isDateFormatEndWithSlash);

		List<Date> dates = new ArrayList<Date>();

		int dateFormatOccurIdx = reflectedPathCharCnt + 1;
		dates = extractDatesFromFiles(filesFilteredByDateFormat, dateFormatOccurIdx, dateFormat, fileNameSuffix);

		Collections.sort(dates, Collections.reverseOrder());

		return dates;
	}

	private FilePath[] filterByPrefix(FilePath baseDir, final String fileNamePrefix) {
		FilePath[] filesFilteredByPrefix = null;
		if (fileNamePrefix != null && !fileNamePrefix.isEmpty()) {
			final int lastSlashInPrefixIdx = fileNamePrefix.lastIndexOf("/");
			if (lastSlashInPrefixIdx > 0)
				baseDir = baseDir.newFilePath(fileNamePrefix.substring(0, lastSlashInPrefixIdx));

			filesFilteredByPrefix = baseDir.listFiles(new FilePathNameFilter() {
				@Override
				public boolean accept(FilePath dir, String name) {
					if (fileNamePrefix.length() == lastSlashInPrefixIdx + 1)
						return true;

					return name.startsWith(fileNamePrefix.substring(lastSlashInPrefixIdx + 1));
				}
			});
		} else {
			filesFilteredByPrefix = baseDir.listFiles();
		}

		return filesFilteredByPrefix;
	}

	private List<FilePath> filterByDateFormat(FilePath[] files, SimpleDateFormat[] splitedDateFormatArr, int splitedDateFormatIdx,
			int reflectedPathCharCnt, boolean isDateFormatEndWithSlash) {

		List<FilePath> filteredFiles = new ArrayList<FilePath>();
		if (files == null)
			return filteredFiles;

		for (FilePath file : files) {

			String targetName = file.getAbsolutePath().substring(reflectedPathCharCnt + 1);

			Date dayForCompare;
			try {
				dayForCompare = splitedDateFormatArr[splitedDateFormatIdx].parse(targetName);
				boolean isFullMatched = splitedDateFormatArr[splitedDateFormatIdx].format(dayForCompare).equals(targetName);

				if (splitedDateFormatArr.length == splitedDateFormatIdx + 1) {
					if (!isDateFormatEndWithSlash) {
						filteredFiles.add(file);
					} else if (isFullMatched) {
						filteredFiles.addAll(Arrays.asList(file.listFiles()));
					} else {
						logger.error("araqne logstorage: invalid log filename, {}", file.getAbsoluteFilePath());
					}

				} else if (file.isDirectory() && splitedDateFormatArr.length > splitedDateFormatIdx + 1) {
					if (!isFullMatched) {
						logger.error("araqne logstorage: invalid log filename, {}", file.getAbsoluteFilePath());
						continue;
					}

					int appendedLen = 1;
					appendedLen += targetName.length();

					filteredFiles.addAll(filterByDateFormat(file.listFiles(), splitedDateFormatArr, splitedDateFormatIdx + 1,
							reflectedPathCharCnt + appendedLen,
							isDateFormatEndWithSlash));
				} else {
					logger.error("araqne logstorage: invalid log filename, {}", file.getAbsoluteFilePath());
				}
			} catch (ParseException e1) {
				logger.error("araqne logstorage: invalid log filename, {}", file.getAbsoluteFilePath());
			}
		}
		return filteredFiles;
	}

	private List<Date> extractDatesFromFiles(List<FilePath> files, int dateFormatOccurIdx, SimpleDateFormat dateFormat,
			String fileNameSuffix) {
		List<Date> dates = new ArrayList<Date>();
		String[] suffixSplitedBySlash = fileNameSuffix.split("/");

		// assign current year to date
		Calendar yearModifier = null;
		if (!dateFormat.toPattern().contains("yyyy")) {
			yearModifier = Calendar.getInstance();
			yearModifier.setTimeZone(dateFormat.getTimeZone());
		}

		ListIterator<FilePath> li = files.listIterator();
		while (li.hasNext()) {
			FilePath file = li.next();
			String targetName = file.getAbsolutePath().substring(dateFormatOccurIdx);
			targetName = targetName.replace("\\", "/");
			Date d = null;
			try {
				d = dateFormat.parse(targetName);
				if (dates.contains(d))
					continue;

				String dateString = dateFormat.format(d);
				if (isMatchedWithSuffix(file, suffixSplitedBySlash, 0, dateFormatOccurIdx + dateString.length())) {

					if (yearModifier != null) {
						int year = Calendar.getInstance().get(Calendar.YEAR);
						yearModifier.setTime(d);
						yearModifier.set(Calendar.YEAR, year);
						d = yearModifier.getTime();
					}

					dates.add(d);
				} else {
					logger.error("araqne logstorage: invalid log filename, {}", file.getAbsoluteFilePath());
				}
			} catch (ParseException e1) {
				logger.error("araqne logstorage: invalid log filename while exracting {}", file.getAbsoluteFilePath());
			}
		}
		return dates;
	}

	private boolean compareTargetAndSuffix(String target, String suffix) {
		if (suffix == null || suffix.isEmpty()) {
			return target == null || target.isEmpty();
		}
		else {
			return target.matches(suffix);
		}
	}

	private boolean isMatchedWithSuffix(FilePath file, String[] suffixSplitedBySlash, int splitedSuffixIdx, int checkedLength) {
		boolean isMatched = false;

		String targetStr = file.getAbsolutePath().substring(checkedLength);

		if (compareTargetAndSuffix(targetStr, suffixSplitedBySlash[splitedSuffixIdx])) {
			if (suffixSplitedBySlash.length == splitedSuffixIdx + 1) {
				return file.isFile();
			}
			else if (file.isDirectory()) {
				for (FilePath subFile : file.listFiles()) {
					isMatched = isMatchedWithSuffix(subFile, suffixSplitedBySlash, splitedSuffixIdx + 1,
							checkedLength + targetStr.length() + 1);
					if (isMatched)
						break;
				}
			}
		}

		return isMatched;
	}

	@Override
	public LogFileWriter newWriter(Map<String, Object> options) {
		checkOption(options);
		String tableName = (String) options.get(OPT_TABLE_NAME);
		Date day = (Date) options.get(OPT_DAY);
		FilePath indexPath = getFilePath(options, OPT_INDEX_PATH);
		FilePath dataPath = getFilePath(options, OPT_DATA_PATH);
		CallbackSet cbSet = (CallbackSet) options.get(OPT_CALLBACK_SET);
		AtomicLong lastKey = (AtomicLong) options.get(OPT_LASTKEY);
		try {
			return new LogFileWriterTxt(indexPath, dataPath, cbSet, tableName, day, lastKey);
		} catch (Throwable t) {
			throw new IllegalStateException("cannot open writer txt: data file - " + dataPath.getAbsolutePath(), t);
		}
	}

	private void checkOption(Map<String, Object> options) {
		for (String key : new String[] { OPT_INDEX_PATH, OPT_DATA_PATH }) {
			if (!options.containsKey(key))
				throw new IllegalArgumentException("LogFileServiceV1: " + key + " must be supplied");
		}
	}

	private FilePath getFilePath(Map<String, Object> options, String optName) {
		Object obj = options.get(optName);
		if (obj == null)
			return (FilePath) obj;
		else if (obj instanceof File) {
			return new LocalFilePath((File) obj);
		} else {
			return (FilePath) obj;
		}
	}

	@Override
	public LogFileReader newReader(String tableName, Map<String, Object> options) {
		checkOption(options);
		Date day = (Date) options.get("day");

		TableSchema schema = tableRegistry.getTableSchema(tableName, true);

		StorageConfig primaryStorage = schema.getPrimaryStorage();

		String fileNamePrefix = retrieveConfig(primaryStorage, "filename_prefix", "");
		String dateFormatString = retrieveConfig(primaryStorage, "date_format", "");
		String fileNameSuffix = retrieveConfig(primaryStorage, "filename_suffix", "");

		String dateLocale = retrieveConfig(primaryStorage, "date_locale", "en");
		SimpleDateFormat dateFormat = new SimpleDateFormat(dateFormatString, new Locale(dateLocale));

		String timeZone = retrieveConfig(primaryStorage, "timezone", "");
		if (!timeZone.isEmpty()) {
			if (TimeZoneMappings.getTimeZone(timeZone) != null)
				timeZone = (String) TimeZoneMappings.getTimeZone(timeZone);
			dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
		}

		FilePath baseDir = logDir.newFilePath(Integer.toString(schema.getId()));
		if (schema.getPrimaryStorage().getBasePath() != null)
			baseDir = storageManager.resolveFilePath(schema.getPrimaryStorage().getBasePath());

		String parentFileName = fileNamePrefix + dateFormat.format(day);

		List<FilePath> dataPathList = new ArrayList<FilePath>();

		if (fileNameSuffix == null || fileNameSuffix.isEmpty()) {
			FilePath dataPath = new LocalFilePath(baseDir.getAbsolutePath() + "/" + parentFileName);
			if (dataPath.isFile())
				dataPathList.add(dataPath);
		} else {
			String parentPathStr = baseDir.getAbsolutePath() + "/" + parentFileName.replace("\\", "/");
			int lastBackslashIdx = parentPathStr.lastIndexOf("/");
			final String remain = parentPathStr.substring(lastBackslashIdx + 1);

			FilePath parentPath = new LocalFilePath(parentPathStr.substring(0, lastBackslashIdx));

			String[] suffixList = convertToRegex(fileNameSuffix).split("/");

			dataPathList.addAll(retrieveFilesFromDate(parentPath.listFiles(new FilePathNameFilter() {
				@Override
				public boolean accept(FilePath dir, String name) {
					return name.startsWith(remain);
				}
			}), suffixList, 0, parentPathStr.length()));
		}

		String charset = retrieveConfig(primaryStorage, "charset", "utf-8");

		try {
			return new LogFileReaderTxt(tableName, dataPathList, day, charset);
		} catch (Throwable t) {
			throw new IllegalStateException("cannot open reader txt: data file - " + dataPathList.get(0).getAbsolutePath());
		}
	}

	private List<FilePath> retrieveFilesFromDate(FilePath[] files, String[] suffixList, int suffixListIdx, int recognizedCharCnt) {
		List<FilePath> matchedWithSuffix = new ArrayList<FilePath>();

		if (files == null)
			return matchedWithSuffix;
		
		for (FilePath file : files) {
			String targetString = file.getAbsolutePath().substring(recognizedCharCnt);

			if (compareTargetAndSuffix(targetString, suffixList[suffixListIdx])) {
				if (suffixList.length == suffixListIdx + 1) {
					matchedWithSuffix.add(file);
				} else {
					matchedWithSuffix.addAll(retrieveFilesFromDate(file.listFiles(), suffixList, suffixListIdx + 1,
							recognizedCharCnt + targetString.length() + 1));
				}
			}
		}
		return matchedWithSuffix;
	}

	@Override
	public List<TableConfigSpec> getConfigSpecs() {
		TableConfigSpec fileNamePrefix = newTableConfigSpec("filename_prefix", true, TableConfigSpec.locales("File name prefix", "파일명 접두사"));
		TableConfigSpec dateFormat = newTableConfigSpec("date_format", false, TableConfigSpec.locales("Date format", "날짜 형식"));
		TableConfigSpec fileNameSuffix = newTableConfigSpec("filename_suffix", true, TableConfigSpec.locales("File name suffix", "파일명 접미사"));
		TableConfigSpec dateLocale = newTableConfigSpec("date_locale", true, TableConfigSpec.locales("Date locale", "날짜 로케일"));
		TableConfigSpec timeZone = newTableConfigSpec("timezone", true, TableConfigSpec.locales("Time zone", "시간대"));
		TableConfigSpec charSet = newTableConfigSpec("charset", true, TableConfigSpec.locales("Charset", "문자집합"));

		return Arrays.asList(fileNamePrefix, dateFormat, fileNameSuffix, dateLocale, timeZone, charSet);
	}

	private TableConfigSpec newTableConfigSpec(String key, boolean optional, Map<Locale, String> displayNames) {
		TableConfigSpec newTableConfigSpec = new TableConfigSpec();
		newTableConfigSpec.setKey(key);
		newTableConfigSpec.setOptional(optional);
		newTableConfigSpec.setDisplayNames(displayNames);

		return newTableConfigSpec;
	}

	@Override
	public Map<String, String> getConfigs() {
		return new HashMap<String, String>();
	}

	@Override
	public void setConfig(String key, String value) {
	}

	@Override
	public void unsetConfig(String key) {
	}

	@Override
	public List<TableConfigSpec> getReplicaConfigSpecs() {
		return Arrays.asList();
	}

	@Override
	public List<TableConfigSpec> getSecondaryConfigSpecs() {
		return Arrays.asList();
	}

}
