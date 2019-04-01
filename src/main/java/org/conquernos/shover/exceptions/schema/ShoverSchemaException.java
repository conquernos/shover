package org.conquernos.shover.exceptions.schema;

import org.conquernos.shover.exceptions.ShoverException;

public class ShoverSchemaException extends ShoverException {

	public ShoverSchemaException() {
	}

	public ShoverSchemaException(String message) {
		super(message);
	}

	public ShoverSchemaException(Throwable cause) {
		super(cause);
	}

	public ShoverSchemaException(String message, Throwable cause) {
		super(message, cause);
	}

}
