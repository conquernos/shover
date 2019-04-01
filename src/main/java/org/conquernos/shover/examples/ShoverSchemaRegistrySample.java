package org.conquernos.shover.examples;


import org.conquernos.shover.exceptions.ShoverException;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import java.io.IOException;

public class ShoverSchemaRegistrySample {

	public static void main(String[] args) throws ShoverException {
		String host = args[0];
//		String host = "http://172.19.32.37:8070";
		String subject = args[1];
		String schemaJson = args[2];

		CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(host, 10);

//		String subject = "pTest";
//		String schemaJson = "{" +
//			"\"type\": \"record\"" +
//			", \"name\": \"pTestValue\"" +
//			", \"fields\": [ { \"name\": \"log\", \"type\": \"string\" } ]" +
//			"}";
//		String subject = "logFileTest";
//		String schemaJson = "{" +
//			"\"type\": \"record\"" +
//			", \"name\": \"logFileTestValue\"" +
//			", \"fields\": [ { \"name\": \"log\", \"type\": \"string\" } ]" +
//			"}";
//		String subject = "document";
//		String schemaJson = "{" +
//			"\"type\": \"record\"" +
//			", \"name\": \"documentValue\"" +
//			", \"fields\": [ { \"name\": \"url\", \"type\": \"string\" }, { \"name\": \"document\", \"type\": \"string\" } ]" +
//			"}";

		System.out.println("schema-registry : " + host);
		System.out.println("subject : " + subject);
		System.out.println("schema : " + schemaJson);

		Schema schema = new Schema.Parser().parse(schemaJson);
		try {
			int id = schemaRegistryClient.register(subject, schema);
			System.out.println("registered schema id : " + id);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (RestClientException e) {
			e.printStackTrace();
		}
	}

}
