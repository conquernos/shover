package org.conquernos.shover.exceptions.topic;


public class ShoverNullTopicException extends ShoverTopicException {

	public ShoverNullTopicException() {
	}

	public ShoverNullTopicException(String message) {
		super(message);
	}

	public ShoverNullTopicException(Throwable cause) {
		super(cause);
	}

	public ShoverNullTopicException(String message, Throwable cause) {
		super(message, cause);
	}

}
