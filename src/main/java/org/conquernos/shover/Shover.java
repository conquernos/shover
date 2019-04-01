package org.conquernos.shover;


import org.conquernos.shover.config.ShoverConfig;
import org.conquernos.shover.exceptions.ShoverException;
import org.conquernos.shover.exceptions.schema.ShoverSchemaNotExistException;
import org.conquernos.shover.exceptions.send.ShoverMessageException;
import org.conquernos.shover.exceptions.send.ShoverSendException;
import org.conquernos.shover.exceptions.topic.ShoverNullTopicException;
import org.conquernos.shover.schema.CachedSchemas;
import org.conquernos.shover.schema.Subject;
import org.conquernos.shover.stats.ShoverStats;
import org.conquernos.shover.utils.Loader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.concurrent.TimeUnit;


/**
 * A Kafka producer that publishes records to the Kafka cluster.
 * The Shover is thread-safe and the singleton class.
 * An instance need only a config file. So the directory of the file 'shover.conf' is in classpath
 * or set the system properties 'key:shover.config, value:path'. (ex. -Dshover.config=/path/shover.conf)
 * TODO 1. 스키마 레지스트리에 스키마가 변경 되어서 적용하고자 할 때 application 재시작하지 않고 적용될 수 있도록 기능 추가 (dynamic configuration)
 * TODO 2. 정상종료 되지 못 했을 경우를 대비해서 데이터 백업 기능 추가
 */
public class Shover {

	// class for graceful shutdown
	private static class ShutdownShover extends Thread {

		private static final Logger logger = LoggerFactory.getLogger(ShutdownShover.class);

		private final Shover shover;

		ShutdownShover(Shover shover) {
			this.shover = shover;
		}

		@Override
		public void run() {
			// close producer
			shover.close();

			logger.info("Shover stats : {}", shover.stats);
			logger.info("Shover was shut down.");
		}

	}

	private static final Logger logger = LoggerFactory.getLogger(Shover.class);

	private static Shover shover = null;

	private final ShoverConfig config;

	private final KafkaProducer<Object, Object> producer;

	private final CachedSchemas cachedSchemas;

	private final ShoverStats stats = new ShoverStats();


	private Shover(String configFilePath) {
		this(new ShoverConfig(configFilePath));
	}

	private Shover(ShoverConfig config) {
		this.config = config;

		try {
			// schemas are cached only one time
			cachedSchemas = new CachedSchemas(config.getSchemaRegistryUrl(), config.getSubjects());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		producer = new KafkaProducer<>(config.getProducerProperties());

		Runtime.getRuntime().addShutdownHook(new ShutdownShover(this));
	}

	/**
	 * Get the shover instance.
	 * @return Shover instance
	 */
	public static Shover getInstance() {
		if (shover == null) {
			String configFilePath = System.getProperty(ShoverConfig.CONFIG_FILE_NAME_KEY);
			if (configFilePath == null) {
				URL url = Loader.getResource(ShoverConfig.DEFAULT_CONFIG_FILE_NAME);
				if (url == null) throw new RuntimeException("Config file is not found : " + ShoverConfig.DEFAULT_CONFIG_FILE_NAME);

				configFilePath = url.getPath();
			} else {
				if (!new File(configFilePath).exists()) throw new RuntimeException("Config file is not found : " + configFilePath);
			}

			shover = new Shover(configFilePath);
		}

		return shover;
	}

	/**
	 * Asynchronously send a message to the kafka cluster.
	 * The topic of this message is the first value of the topics property. (ex. topics=topic1,topic2,... -> topic1)
	 * And the schema version is the latest version.
	 * So if want to select other topic, use {@link #send(String, ShoverMessage)}.
	 * @param message A value object that takes message (the member values of the class have to correspond with the schema)
	 * @throws ShoverException If the message was not sent
	 */
	public void send(ShoverMessage message) throws ShoverException {
		send(config.getSubjects()[0].getSubject(), message);
	}

	/**
	 * Asynchronously send a message to the kafka cluster.
	 * The topic of this message is the first value of the topics property. (ex. topics=topic1,topic2,... -> topic1)
	 * And the schema version is the latest version.
	 * So if want to select other topic, use {@link #send(String, Object[])}.
	 * @param values Values that is in the order of their schema positions
	 * @throws ShoverException If the message was not sent
	 */
	public void send(Object[] values) throws ShoverException {
		send(config.getSubjects()[0].getSubject(), values);
	}

	/**
	 * Asynchronously send a message to the kafka cluster.
	 * The schema version is the latest version.
	 * So if want to select other version, use {@link #send(String, int, ShoverMessage)}.
	 * @param topic A topic of this message
	 * @param message A value object that takes message (the member values of the class has to match the schema)
	 * @throws ShoverException
	 */
	public void send(String topic, ShoverMessage message) throws ShoverException {
		send(topic, 0, message);
	}

	/**
	 * Asynchronously send a message to the kafka cluster.
	 * The schema version is the latest version.
	 * So if want to select other version, use {@link #send(String, int, Object[])}.
	 * @param topic A topic of this message
	 * @param values Values that is in the order of their schema positions
	 * @throws ShoverException
	 */
	public void send(String topic, Object[] values) throws ShoverException {
		send(topic, 0, values);
	}

	/**
	 * Asynchronously send a message to the kafka cluster.
	 * @param topic A topic of this message
	 * @param version A schema version of this message
	 * @param message A value object that takes message (the member values of the class has to match the schema)
	 * @throws ShoverException
	 */
	public void send(String topic, int version, ShoverMessage message) throws ShoverException {
		send(topic, version, (Object) message);
	}

	/**
	 * Asynchronously send a message to the kafka cluster.
	 * @param topic A topic of this message
	 * @param version A schema version of this message
	 * @param message Values that is in the order of their schema positions
	 * @throws ShoverException
	 */
	public void send(String topic, int version, Object[] message) throws ShoverException {
		send(topic, version, (Object) message);
	}

	/**
	 * Asynchronously send a message to the kafka cluster.
	 * @param topic A topic of this message
	 * @param version A schema version of this message
	 * @param message Values that is in the order of their schema positions
	 * @throws ShoverException
	 */
	protected void send(String topic, int version, Object message) throws ShoverException {
		if (topic == null) throw new ShoverNullTopicException();

		// use only cached schemas
		Schema schema = cachedSchemas.getCachedSchema(new Subject(topic, version));
		if (schema == null) throw new ShoverSchemaNotExistException();

		ProducerRecord<Object, Object> record;
		try {
			Object value;
			if (message instanceof ShoverMessage) {
				value = convertToValueRecord((ShoverMessage) message, schema);
			} else if (message instanceof Object[]) {
				value = convertToValueRecord((Object[]) message, schema);
			} else {
				throw new ShoverMessageException(topic, schema, message);
			}
			record = new ProducerRecord<>(topic, value);
		} catch (Exception e) {
			throw new ShoverMessageException(topic, schema, message, e);
		}

		try {
			stats.addNumberOfMessages(1);

			producer.send(record, (metadata, exception) -> {
				if (exception == null) {
					stats.addNumberOfCompletedMessages(1);
				} else {
					logger.error("producer send error", exception);
				}
			});

			// TO DO : flush
			flush();

		} catch (Exception e) {
			throw new ShoverSendException(topic, message, e);
		}
	}

	/**
	 * Get the number of total messages that have been delivered to 'send()' method
	 * @return the number of total messages
	 */
	public long getNumberOfMessages() {
		return stats.getNumberOfMessages();
	}

	/**
	 * Get the number of total messages that have been delivered to the Kafka broker (flushed)
	 * @return the number of total messages
	 */
	public long getNumberOfCompletedMessages() {
		return stats.getNumberOfCompletedMessages();
	}

	private Object convertToValueRecord(ShoverMessage message, Schema schema) throws NoSuchFieldException, IllegalAccessException {
		GenericRecord record = new GenericData.Record(schema);
		Class<?> clazz = message.getClass();
		for (Schema.Field schemaField : schema.getFields()) {
			Field field = clazz.getField(schemaField.name());
			field.setAccessible(true);
			record.put(schemaField.name(), field.get(message));
		}

		return record;
	}

	private Object convertToValueRecord(Object[] message, Schema schema) {
		GenericRecord record = new GenericData.Record(schema);
		int idx = 0;
		for (Schema.Field schemaField : schema.getFields()) {
			record.put(schemaField.name(), message[idx++]);
		}

		return record;
	}

	private void flush() {
		// if the number of stacked messages is more than the flush size
		if ( (stats.getNumberOfMessages() - stats.getNumberOfCompletedMessages()) >= config.getMessageFlushSize()) {
			producer.flush();
		}

		// TODO : 시간이 설장한 값만큼 지나면 flush 되도록 추가
	}

	private void close() {
		producer.close(config.getShutdownTimeout(), TimeUnit.SECONDS);
	}

}
