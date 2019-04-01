package org.conquernos.shover.exceptions.config;


public class ShoverConfigException extends RuntimeException {

	public ShoverConfigException() {
	}

	public ShoverConfigException(String message) {
		super(message);
	}

	public ShoverConfigException(String message, Throwable cause) {
		super(message, cause);
	}

	public ShoverConfigException(Throwable cause) {
		super(cause);
	}

	public static class WrongPathOrNullValue extends ShoverConfigException {

		public WrongPathOrNullValue(String path) {
			super("ShoverConfigException : must have the path '" + path + "' and value");
		}

	}

	public static class WrongTypeValue extends ShoverConfigException {

		public WrongTypeValue(String path) {
			super("ShoverConfigException : the value of the path '" + path + "' is wrong type");
		}

	}

}
