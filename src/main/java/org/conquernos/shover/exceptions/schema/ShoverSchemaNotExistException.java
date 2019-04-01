package org.conquernos.shover.exceptions.schema;


public class ShoverSchemaNotExistException extends ShoverSchemaException {

	public ShoverSchemaNotExistException() {
	}

	public ShoverSchemaNotExistException(String message) {
		super(message);
	}

	public ShoverSchemaNotExistException(Throwable cause) {
		super(cause);
	}

	public ShoverSchemaNotExistException(String message, Throwable cause) {
		super(message, cause);
	}

}
