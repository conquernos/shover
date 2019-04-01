package org.conquernos.shover.exceptions.send;


import org.conquernos.shover.ShoverMessage;
import org.conquernos.shover.exceptions.ShoverException;

public class ShoverSendException extends ShoverException {

	public ShoverSendException(String topic, Object message) {
		super("{\n\ttopic : " + topic + "\n\t, message : " + message + "\n}\n");
	}

	public ShoverSendException(String topic, Object message, Throwable cause) {
		super("{\n\ttopic : " + topic + "\n\t, message : " + message + "\n}\n", cause);
	}

	public ShoverSendException(String topic, ShoverMessage message) {
		super("{\n\ttopic : " + topic + "\n\t, message : " + message + "\n}\n");
	}

	public ShoverSendException(String topic, ShoverMessage message, Throwable cause) {
		super("{\n\ttopic : " + topic + "\n\t, message : " + message + "\n}\n", cause);
	}

	public ShoverSendException(String topic, Object[] message) {
		super("{\n\ttopic : " + topic + "\n\t, message : " + message + "\n}\n");
	}

	public ShoverSendException(String topic, Object[] message, Throwable cause) {
		super("{\n\ttopic : " + topic + "\n\t, message : " + message + "\n}\n", cause);
	}

}
