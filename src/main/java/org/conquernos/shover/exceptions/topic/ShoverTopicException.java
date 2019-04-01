package org.conquernos.shover.exceptions.topic;

import org.conquernos.shover.exceptions.ShoverException;

public class ShoverTopicException extends ShoverException {

	public ShoverTopicException() {
	}

	public ShoverTopicException(String message) {
		super(message);
	}

	public ShoverTopicException(Throwable cause) {
		super(cause);
	}

	public ShoverTopicException(String message, Throwable cause) {
		super(message, cause);
	}

}
