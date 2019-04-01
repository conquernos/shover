package org.conquernos.shover.schema;


import org.conquernos.shover.exceptions.schema.ShoverSchemaException;
import org.conquernos.shover.exceptions.schema.ShoverSchemaRegistryException;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.Map;

public class CachedSchemas {

	private final Map<Subject, Schema> subjectCache = new HashMap<>();

	private final String schemaRegistryUrl;

	public CachedSchemas(String schemaRegistryUrl, Subject[] subjects) throws ShoverSchemaException {
		this.schemaRegistryUrl = schemaRegistryUrl;

		try {
			ShoverSchemaRegistryClient client = new ShoverSchemaRegistryClient(schemaRegistryUrl);
			for (Subject subject : subjects) {
				Schema schema;
				if (subject.isLatestVersion()) {
					schema = getLatestSchema(client, subject.getSubject());
				} else {
					schema = getSchema(client, subject);
				}
				subjectCache.put(subject, schema);
			}
		} catch (Exception e) {
			throw new ShoverSchemaRegistryException(e);
		}
	}

	/*
	Cached
	 */
	public Schema getCachedLatestSchema(String topic) {
		return subjectCache.get(new Subject(topic));
	}

	/*
	Cached
	 */
	public Schema getCachedSchema(Subject subject) {
		return subjectCache.get(subject);
	}

	public void update() throws ShoverSchemaException {
		ShoverSchemaRegistryClient client = new ShoverSchemaRegistryClient(schemaRegistryUrl);
		for (Subject subject : subjectCache.keySet()) {
			try {
				subjectCache.put(subject, subject.isLatestVersion()
					? getLatestSchema(client, subject.getSubject()) : getSchema(client, subject));
			} catch (Exception e) {
				throw new ShoverSchemaException(e);
			}
		}
	}

	private Schema getLatestSchema(ShoverSchemaRegistryClient client, String subject) throws ShoverSchemaException {
		SchemaMetadata meta;
		try {
			meta = client.getLatestSchemaMetadata(subject);
		} catch (Exception e) {
			throw new ShoverSchemaException(e);
		}
		return new Schema.Parser().parse(meta.getSchema());
	}

	private Schema getSchema(ShoverSchemaRegistryClient client, Subject subject) throws ShoverSchemaException {
		SchemaMetadata meta;
		try {
			meta = client.getSchemaMetadata(subject.getSubject(), subject.getVersion());
		} catch (Exception e) {
			throw new ShoverSchemaException(e);
		}
		return new Schema.Parser().parse(meta.getSchema());
	}

}
