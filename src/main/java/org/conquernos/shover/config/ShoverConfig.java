package org.conquernos.shover.config;


import org.conquernos.shover.exceptions.config.ShoverConfigException;
import org.conquernos.shover.schema.Subject;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.conquernos.shover.utils.StringUtils.*;

public class ShoverConfig {

	private final Properties properties;

	public static final String CONFIG_FILE_NAME_KEY = "shover.config";
	public static final String DEFAULT_CONFIG_FILE_NAME = "shover.conf";

	private static final String PROP_TOPICS = "topics";
	private static final String PROP_BROKERS = "brokers";
	private static final String PROP_SCHEMA_REGISTRY_URL = "schema.registry.url";
	private static final String PROP_MESSAGE_FLUSH_SIZE = "message.flush.size";
	private static final String PROP_SHUTDOWN_TIMEOUT = "shutdown.timeout";

	private final Subject[] subjects;
	private final String schemaRegistryUrl;
	private final int messageFlushSize;
	private final int shutdownTimeout;

	private final String brokers;
	private final String keySerializer;
	private final String valueSerializer;

	public ShoverConfig(String configFilePath) {
		try {
			properties = Utils.loadProps(configFilePath);
			subjects = toSubjects(getStringListFromConfig(properties, PROP_TOPICS, true));
			schemaRegistryUrl = getStringFromConfig(properties, PROP_SCHEMA_REGISTRY_URL, true);
			messageFlushSize = getIntegerFromConfig(properties, PROP_MESSAGE_FLUSH_SIZE, 100);
			shutdownTimeout = getIntegerFromConfig(properties, PROP_SHUTDOWN_TIMEOUT, 3);

			brokers = getStringFromConfig(properties, PROP_BROKERS, true);
			keySerializer = "io.confluent.kafka.serializers.KafkaAvroSerializer";
			valueSerializer = "io.confluent.kafka.serializers.KafkaAvroSerializer";
		} catch (Exception e) {
			throw new ShoverConfigException(e);
		}
	}

	public Subject[] getSubjects() {
		return subjects;
	}

	public String getSchemaRegistryUrl() {
		return schemaRegistryUrl;
	}

	public int getMessageFlushSize() {
		return messageFlushSize;
	}

	public int getShutdownTimeout() {
		return shutdownTimeout;
	}

	private String getBrokers() {
		return brokers;
	}

	private String getKeySerializer() {
		return keySerializer;
	}

	private String getValueSerializer() {
		return valueSerializer;
	}

	public Properties getProducerProperties() {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokers());
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, getKeySerializer());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getValueSerializer());
		properties.put("schema.registry.url", getSchemaRegistryUrl());

		return properties;
	}

	private Subject[] toSubjects(List<String> topics) {
		Subject[] subjects = new Subject[topics.size()];

		int idx=0;
		for (String topic : topics) {
			if (topic.indexOf(':') == -1) {
				subjects[idx] = new Subject(topic);
			} else {
				String[] parts = topic.split(":");
				subjects[idx] = new Subject(parts[0], toVersion(parts[1]));
			}

			idx++;
		}

		return subjects;
	}

	private int toVersion(String part) {
		if (!part.startsWith("v")) {
			part = part.substring(1);
			try {
				int version = Integer.parseInt(part);
				if (version < 1) throw new ShoverConfigException("a version must be bigger than zero");

				return version;
			} catch (NumberFormatException e) {
				throw new ShoverConfigException("a version expression must be 'v' + integer", e);
			}
		} else {
			throw new ShoverConfigException("a version expression must be started with 'v' character");
		}
	}

	protected static String getStringFromConfig(Properties config, String path, String defaultValue) {
		String value = getStringFromConfig(config, path, false);
		return (value == null)? defaultValue : value;
	}

	protected static String getStringFromConfig(Properties config, String path, boolean notNull) {
		final char[] trimChars = new char[] {' ', '\"'};
		String value = null;
		if (config.containsKey(path)) {
			value = config.getProperty(path);
			if (value != null) {
				value = trim(value, trimChars);
				if (value.length() == 0) value = null;
			}
		}

		if (notNull && value == null) throw new ShoverConfigException.WrongPathOrNullValue(path);

		return value;
	}

	protected static Integer getIntegerFromConfig(Properties config, String path, Integer defaultValue) {
		Integer value = getIntegerFromConfig(config, path, false);
		return (value == null)? defaultValue : value;
	}

	protected static Integer getIntegerFromConfig(Properties config, String path, boolean notNull) {
		String value = getStringFromConfig(config, path, notNull);
		if (value == null) return null;
		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException e) {
			throw new ShoverConfigException.WrongTypeValue(path);
		}
	}

	protected static Double getDoubleFromConfig(Properties config, String path, Double defaultValue) {
		Double value = getDoubleFromConfig(config, path, false);
		return (value == null)? defaultValue : value;
	}

	protected static Double getDoubleFromConfig(Properties config, String path, boolean notNull) {
		String value = getStringFromConfig(config, path, notNull);
		if (value == null) return null;

		try {
			return Double.parseDouble(value);
		} catch (NumberFormatException e) {
			throw new ShoverConfigException.WrongTypeValue(path);
		}
	}

	protected static List<String> getStringListFromConfig(Properties config, String path, List<String> defaultValue) {
		List<String> values = getStringListFromConfig(config, path, false);
		return (values == null)? defaultValue : values;
	}

	protected static List<String> getStringListFromConfig(Properties config, String path, boolean notNull) {
		List<String> values = null;
		String value = getStringFromConfig(config, path, notNull);
		if (value == null) return null;

		for (String part : value.split(",")) {
			part = part.trim();
			if (part.length() > 0) {
				if (values == null) values = new ArrayList<>();
				values.add(part);
			}
		}

		if (notNull && values == null) throw new ShoverConfigException.WrongPathOrNullValue(path);

		return values;
	}

}
