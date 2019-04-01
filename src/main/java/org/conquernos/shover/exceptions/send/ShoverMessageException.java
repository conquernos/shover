package org.conquernos.shover.exceptions.send;


import org.conquernos.shover.ShoverMessage;
import org.conquernos.shover.exceptions.ShoverException;
import org.apache.avro.Schema;

public class ShoverMessageException extends ShoverException {

	public ShoverMessageException(String topic, Schema schema, Object message) {
		super("{\n\ttopic : " + topic + "\n\t, schema : " + schema.toString(false) + "\n\t, message : " + message + "\n}\n");
	}

	public ShoverMessageException(String topic, Schema schema, Object message, Throwable cause) {
		super("{\n\ttopic : " + topic + "\n\t, schema : " + schema.toString(false) + "\n\t, message : " + message + "\n}\n", cause);
	}

	public ShoverMessageException(String topic, Schema schema, ShoverMessage message) {
		super("{\n\ttopic : " + topic + "\n\t, schema : " + schema.toString(false) + "\n\t, message : " + message + "\n}\n");
	}

	public ShoverMessageException(String topic, Schema schema, ShoverMessage message, Throwable cause) {
		super("{\n\ttopic : " + topic + "\n\t, schema : " + schema.toString(false) + "\n\t, message : " + message + "\n}\n", cause);
	}

	public ShoverMessageException(String topic, Schema schema, Object[] message) {
		super("{\n\ttopic : " + topic + "\n\t, schema : " + schema.toString(false) + "\n\t, message : " + message + "\n}\n");
	}

	public ShoverMessageException(String topic, Schema schema, Object[] message, Throwable cause) {
		super("{\n\ttopic : " + topic + "\n\t, schema : " + schema.toString(false) + "\n\t, message : " + message + "\n}\n", cause);
	}

}
