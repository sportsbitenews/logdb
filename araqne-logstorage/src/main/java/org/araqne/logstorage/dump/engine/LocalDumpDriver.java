package org.araqne.logstorage.dump.engine;

import static org.araqne.logstorage.dump.DumpConfigSpec.t;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.dump.DumpConfigSpec;
import org.araqne.logstorage.dump.DumpDriver;
import org.araqne.logstorage.dump.DumpManifest;
import org.araqne.logstorage.dump.DumpService;
import org.araqne.logstorage.dump.ExportRequest;
import org.araqne.logstorage.dump.ExportWorker;
import org.araqne.logstorage.dump.ImportRequest;
import org.araqne.logstorage.dump.ImportWorker;

@Component(name = "logstorage-local-dump-driver")
public class LocalDumpDriver implements DumpDriver {

	@Requires
	private LogStorage storage;

	@Requires
	private DumpService dumpService;

	@Validate
	public void start() {
		dumpService.registerDriver(this);
	}

	@Invalidate
	public void stop() {
		if (dumpService != null)
			dumpService.unregisterDriver(this);
	}

	@Override
	public String getType() {
		return "local";
	}

	@Override
	public String getName(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "로컬 백업 파일";
		return "Local Backup File";
	}

	@Override
	public String getDescription(Locale locale) {
		if (locale != null && locale.equals(Locale.KOREAN))
			return "로컬 파일시스템에 백업 파일을 익스포트하거나 임포트합니다.";
		return "Export or import data from local file system";
	}

	@Override
	public List<DumpConfigSpec> getExportSpecs() {
		DumpConfigSpec path = new DumpConfigSpec("path", t("Path", "경로"), t("Export file path", "덤프 파일 경로"), true);
		return Arrays.asList(path);
	}

	@Override
	public List<DumpConfigSpec> getImportSpecs() {
		DumpConfigSpec path = new DumpConfigSpec("path", t("Path", "경로"), t("Export file path", "덤프 파일 경로"), true);
		return Arrays.asList(path);
	}

	@Override
	public DumpManifest readManifest(Map<String, String> params) throws IOException {
		String s = params.get("path");
		if (s == null)
			throw new IllegalArgumentException("path should be not null");

		File path = new File(s);
		if (!path.isFile() || !path.exists())
			throw new IllegalStateException("path not found: " + path.getAbsolutePath());

		if (!path.canRead())
			throw new IllegalStateException("check read permission: " + path.getAbsolutePath());

		ZipFile zipFile = null;
		InputStream is = null;
		try {
			zipFile = new ZipFile(path);
			is = zipFile.getInputStream(new ZipEntry("manifest.json"));
			DumpManifest manifest = DumpManifest.parseJSON(is);
			manifest.setDriverType("local");
			return manifest;
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
				}
			}

			if (zipFile != null) {
				try {
					zipFile.close();
				} catch (IOException e) {
				}
			}
		}
	}

	@Override
	public ExportWorker newExportWorker(ExportRequest req) {
		return new LocalExportWorker(req, dumpService, storage);
	}

	@Override
	public ImportWorker newImportWorker(ImportRequest req) {
		return new LocalImportWorker(req, dumpService, storage);
	}

}
