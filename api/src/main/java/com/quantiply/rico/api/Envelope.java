package com.quantiply.rico.api;

import java.util.HashMap;
import java.util.Map;

public class Envelope<T>{

    private Map<String, String> headers;
    private T payload;
    private boolean isError;
    private String errorMessage;

    public Envelope() {
        this.headers = new HashMap<>();
    }

    public void setHeader(String k, String v){
        headers.put(k, v);
    }

    public void removeHeader(String k){
        headers.remove(k);
    }

    public String getHeader(String key){
        return headers.get(key);
    }

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public boolean isError() {
        return isError;
    }

    public void setError(boolean isError) {
        this.isError = isError;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
        return "Envelope{" +
                "headers=" + headers +
                ", payload=" + payload +
                '}';
    }

}
