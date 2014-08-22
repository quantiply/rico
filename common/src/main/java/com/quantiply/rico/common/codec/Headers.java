package com.quantiply.rico.common.codec;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;

import com.google.common.base.Objects;

public class Headers {

    private final String id;
    private final DateTime occured;
    private final String schemaId;
    private final Map<String, String> kv;
    
    public Headers(String id, DateTime occured, String schemaId, Map<String, String> kv) {
        if (kv == null) {
            kv = new HashMap<String, String>();
        }
        this.id = id;
        this.occured = occured;
        this.schemaId = schemaId;
        this.kv = kv;
    }
    
    public Headers(String id, DateTime occured) {
        this(id, occured, null, null);
    }
    
    /**
     * Using Guava instead of Java 7 helpers in case we
     * need to compile this for Java 6
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
           return false;
        }
        if (getClass() != obj.getClass())
        {
           return false;
        }
        final Headers other = (Headers) obj;
        
        return Objects.equal(this.id, other.id)
                && Objects.equal(this.occured, other.occured)
                && Objects.equal(this.schemaId, other.schemaId)
                && Objects.equal(this.kv, other.kv);
    }
    
    /**
     * Using Guava in case we need to compile this for Java 6
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(
                this.id, this.occured, this.schemaId, this.kv);
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .addValue(this.id)
                .addValue(this.occured)
                .addValue(this.schemaId)
                .addValue(this.kv)
                .toString();
    }
    
    /**
     * The unique id for the message
     */
    public String getId() {
        return id;
    }
    
    /**
     * Time of the event
     */
    public DateTime getOccured() {
        return occured;
    }
    
    /**
     * Optional schema ID for the message body.  Can be null
     */
    public String getSchemaId() {
        return schemaId;
    }
    
    /**
     * Additional Key/value pairs.  Can be empty
     */
    public Map<String, String> getKv() {
        return kv;
    }
}
