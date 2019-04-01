package org.conquernos.shover.schema;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Collection;
import java.util.List;


public class ShoverSchemaRegistryClient implements SchemaRegistryClient {

	private final RestService restService;

	public ShoverSchemaRegistryClient(String baseUrl) {
		restService = new RestService(baseUrl);
	}

	public ShoverSchemaRegistryClient(List<String> baseUrls) {
		restService = new RestService(baseUrls);
	}

	public int register(String subject, Schema schema) throws IOException, RestClientException {
		return restService.registerSchema(schema.toString(), subject);
	}

	public Schema getByID(int id) throws IOException, RestClientException {
		SchemaString restSchema = restService.getId(id);
		return new Schema.Parser().parse(restSchema.getSchemaString());
	}

	public SchemaMetadata getLatestSchemaMetadata(String subject) throws IOException, RestClientException {
		return toSchemaMetadata(restService.getLatestVersion(subject));
	}

	public SchemaMetadata getSchemaMetadata(String subject, int version) throws IOException, RestClientException {
		return toSchemaMetadata(restService.getVersion(subject, version));
	}

	public int getVersion(String subject, Schema schema) throws IOException, RestClientException{
		return restService.lookUpSubjectVersion(schema.toString(), subject).getVersion();
	}

	public boolean testCompatibility(String subject, Schema schema) throws IOException, RestClientException {
		return restService.testCompatibility(schema.toString(), subject, "latest");
	}

	public String updateCompatibility(String subject, String compatibility) throws IOException, RestClientException {
		ConfigUpdateRequest response = restService.updateCompatibility(compatibility, subject);
		return response.getCompatibilityLevel();
	}

	public String getCompatibility(String subject) throws IOException, RestClientException {
		Config response = restService.getConfig(subject);
		return response.getCompatibilityLevel();
	}

	public Collection<String> getAllSubjects() throws IOException, RestClientException {
		return restService.getAllSubjects();
	}

	private static SchemaMetadata toSchemaMetadata(io.confluent.kafka.schemaregistry.client.rest.entities.Schema response) {
		return new SchemaMetadata(response.getId(), response.getVersion(), response.getSchema());
	}

}

