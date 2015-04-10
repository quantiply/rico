package com.quantiply.rico.errors;

public class ConfigException extends RuntimeException {

    public ConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigException(String message) {
        super(message);
    }

    public ConfigException(Throwable cause) {
        super(cause);
    }

    public ConfigException() {
        super();
    }
}