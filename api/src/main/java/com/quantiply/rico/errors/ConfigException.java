package com.quantiply.rico.errors;

public class ConfigException extends Exception {

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