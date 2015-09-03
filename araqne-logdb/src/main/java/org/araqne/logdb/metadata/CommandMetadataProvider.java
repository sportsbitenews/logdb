package org.araqne.logdb.metadata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Requires;
import org.apache.felix.ipojo.annotations.Validate;
import org.araqne.logdb.FieldOrdering;
import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.MetadataCallback;
import org.araqne.logdb.MetadataProvider;
import org.araqne.logdb.MetadataService;
import org.araqne.logdb.QueryCommandHelp;
import org.araqne.logdb.QueryCommandOption;
import org.araqne.logdb.QueryCommandParser;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.Row;

@Component(name = "logdb-command-metadata")
public class CommandMetadataProvider implements MetadataProvider, FieldOrdering {

	@Requires
	private MetadataService metadataService;

	@Requires
	private QueryParserService queryParserService;

	@Requires
	private FunctionRegistry functionRegistry;

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
	public String getType() {
		return "commands";
	}

	@Override
	public List<String> getFieldOrder() {
		return Arrays.asList("name", "descriptions", "options", "usages");
	}

	@Override
	public void verify(QueryContext context, String queryString) {
	}

	@Override
	public void query(QueryContext context, String queryString, MetadataCallback callback) {
		for (QueryCommandParser command : queryParserService.getCommandParsers()) {
			Map<String, Object> m = new HashMap<String, Object>();
			QueryCommandHelp help = command.getCommandHelp();
			m.put("name", help.getCommandName());

			Map<String, String> desc = new HashMap<String, String>();
			for (Locale locale : help.getDescriptions().keySet())
				desc.put(locale.toString(), help.getDescriptions().get(locale));
			m.put("descriptions", desc);

			Map<String, Map<String, String>> options = new HashMap<String, Map<String, String>>();
			for (String optionName : help.getOptions().keySet()) {
				Map<String, String> option = new HashMap<String, String>();
				QueryCommandOption opt = help.getOptions().get(optionName);
				for (Locale locale : opt.getDescriptions().keySet()) {
					option.put(locale.toString(), opt.getDescriptions().get(locale));
				}
				options.put(optionName, option);
			}
			m.put("options", options);

			Map<String, String> usages = new HashMap<String, String>();
			for (Locale locale : help.getUsages().keySet())
				usages.put(locale.toString(), help.getUsages().get(locale));
			m.put("usages", usages);

			callback.onPush(new Row(m));
		}
	}
}
