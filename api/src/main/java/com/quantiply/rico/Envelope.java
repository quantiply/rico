package com.quantiply.rico;

import java.util.HashMap;
import java.util.Map;

public class Envelope {

    private Map<String, String> headers;
    private Object body;
    private boolean isError;
    private String errorMessage;

    public Envelope() {
        this.headers = new HashMap<>();
    }

    public Envelope(Object body) {
        super();
        setBody(body);
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

    public Object getBody() {
        return body;
    }

    public void setBody(Object body) {
        this.body = body;
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
                ", body=" + body +
                '}';
    }

}
