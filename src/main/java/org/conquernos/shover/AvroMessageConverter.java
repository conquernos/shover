package org.conquernos.shover;


import org.conquernos.shover.arvo.decoder.MapDecoder;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.util.Map;

public class AvroMessageConverter extends AbstractKafkaAvroSerializer {

	private static final DecoderFactory decoderFactory = DecoderFactory.get();

	public static Object jsonToAvro(String json, Schema schema) {
		DatumReader<Object> reader = new GenericDatumReader<>(schema);

		try {
			Object object = reader.read(null, decoderFactory.jsonDecoder(schema, json));

			if (schema.getType().equals(Schema.Type.STRING)) object = object.toString();

			return object;
		} catch (IOException | AvroRuntimeException e) {
			throw new SerializationException(
				String.format("Error deserializing json %s to Avro of schema %s", json, schema), e);
		}
	}

	public static Object mapToAvro(Map<String, Object> map, Schema schema) {
		DatumReader<Object> reader = new GenericDatumReader<>(schema);

		try {
			Object object = reader.read(null, new MapDecoder(schema, map));

			if (schema.getType().equals(Schema.Type.STRING)) object = object.toString();

			return object;
		} catch (IOException | AvroRuntimeException e) {
			throw new SerializationException(
				String.format("Error deserializing map %s to Avro of schema %s", map, schema), e);
		}
	}

}
