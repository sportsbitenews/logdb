package org.araqne.logdb.cep.logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.log.api.AbstractLogger;
import org.araqne.log.api.Log;
import org.araqne.log.api.LoggerFactory;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.LoggerStartReason;
import org.araqne.log.api.LoggerStopReason;
import org.araqne.log.api.Reconfigurable;
import org.araqne.log.api.SimpleLog;
import org.araqne.logdb.Row;
import org.araqne.logdb.cep.Event;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.cep.EventSubscriber;

/**
 * generate event context operation event. e.g. removal, expire, timeout
 * 
 * @author xeraph
 * 
 */
public class CepEventLogger extends AbstractLogger implements EventSubscriber, Reconfigurable {

	private EventContextService eventContextService;

	public CepEventLogger(LoggerSpecification spec, LoggerFactory factory, EventContextService eventContextService) {
		super(spec, factory);

		this.eventContextService = eventContextService;
	}

	@Override
	public void onConfigChange(Map<String, String> oldConfigs, Map<String, String> newConfigs) {
		if (!oldConfigs.get("topics").equals(newConfigs.get("topics"))) {
			setStates(new HashMap<String, Object>());
		}
	}

	@Override
	public boolean isPassive() {
		return true;
	}

	@Override
	protected void onStart(LoggerStartReason reason) {
		String topics = getConfigs().get("topics");
		for (String topic : topics.split(",")) {
			topic = topic.trim();
			eventContextService.addSubscriber(topic, this);
		}
	}

	@Override
	protected void onStop(LoggerStopReason reason) {
		String topics = getConfigs().get("topics");
		for (String topic : topics.split(",")) {
			topic = topic.trim();
			eventContextService.removeSubscriber(topic, this);
		}
	}

	@Override
	protected void runOnce() {
	}

	@Override
	public void onEvent(Event event) {
		Map<String, Object> m = new HashMap<String, Object>();
		List<Object> rows = new ArrayList<Object>(event.getRows().size());

		for (Row row : event.getRows()) {
			rows.add(row.map());
		}

		EventKey key = event.getKey();
		m.put("topic", key.getTopic());
		m.put("key", key.getKey());
		m.put("host", key.getHost());
		m.put("counter", event.getCounter());
		m.put("vars", event.getVariables());
		m.put("created", event.getCreated());
		m.put("cause", event.getCause().toString().toLowerCase());
		m.put("rows", rows);

		Log log = new SimpleLog(new Date(), getFullName(), m);
		write(log);
	}
}
