package org.araqne.logdb.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.FieldOrdering;
import org.araqne.logdb.MetadataCallback;
import org.araqne.logdb.MetadataProvider;
import org.araqne.logdb.MetadataService;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryErrorMessage;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.Row;

@Component(name = "logdb-code-metadata")
public class CodeMetadataProvider implements MetadataProvider, FieldOrdering {

	@Requires
	private MetadataService metadataService;

	@Requires
	private QueryParserService queryParserService;

	@Validate
	public void start() {
		metadataService.addProvider(this);
	}

	@Invalidate
	public void stop() {
		if (metadataService != null)
			metadataService.removeProvider(this);
	}

	@Override
	public List<String> getFieldOrder() {
		return Arrays.asList("code", "en", "ko", "cn", "jp");
	}

	@Override
	public String getType() {
		return "codes";
	}

	@Override
	public void verify(QueryContext context, String queryString) {
	}

	@Override
	public void query(QueryContext context, String queryString, MetadataCallback callback) {
		Map<String, QueryErrorMessage> codeMap = queryParserService.getErrorMessages();
		List<String> codes = new ArrayList<String>(codeMap.keySet());
		Collections.sort(codes);
		for (String code : codes) {
			QueryErrorMessage msg = codeMap.get(code);
			Map<Locale, String> templates = msg.getTemplates();
			String en = templates.get(Locale.ENGLISH);
			String ko = templates.get(Locale.KOREAN);
			String cn = templates.get(Locale.CHINESE);
			String jp = templates.get(Locale.JAPANESE);

			Map<String, Object> m = new HashMap<String, Object>();
			m.put("code", code);

			if (en != null)
				m.put("en", en);

			if (ko != null)
				m.put("ko", ko);

			if (cn != null)
				m.put("cn", cn);

			if (jp != null)
				m.put("jp", jp);

			callback.onPush(new Row(m));
		}
	}
}
