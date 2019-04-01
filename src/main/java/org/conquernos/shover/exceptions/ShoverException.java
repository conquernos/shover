package org.conquernos.shover.exceptions;


public class ShoverException extends Exception {

	public ShoverException() {
	}

	public ShoverException(String message) {
		super(message);
	}

	public ShoverException(Throwable cause) {
		super(cause);
	}

	public ShoverException(String message, Throwable cause) {
		super(message, cause);
	}

}
