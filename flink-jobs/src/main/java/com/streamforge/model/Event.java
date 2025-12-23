package com.streamforge.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents an event in the stream processing pipeline
 */
public class Event implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private final String id;
    private final String type;
    private final String userId;
    private final double value;
    private final long timestamp;
    private final String payload;
    
    @JsonCreator
    public Event(
            @JsonProperty("id") String id,
            @JsonProperty("type") String type,
            @JsonProperty("userId") String userId,
            @JsonProperty("value") double value,
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("payload") String payload) {
        this.id = id;
        this.type = type;
        this.userId = userId;
        this.value = value;
        this.timestamp = timestamp;
        this.payload = payload;
    }
    
    public String getId() {
        return id;
    }
    
    public String getType() {
        return type;
    }
    
    public String getUserId() {
        return userId;
    }
    
    public double getValue() {
        return value;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public String getPayload() {
        return payload;
    }
    
    public boolean isValid() {
        return id != null && !id.isEmpty() &&
               type != null && !type.isEmpty() &&
               userId != null && !userId.isEmpty() &&
               value >= 0 &&
               timestamp > 0;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Double.compare(event.value, value) == 0 &&
               timestamp == event.timestamp &&
               Objects.equals(id, event.id) &&
               Objects.equals(type, event.type) &&
               Objects.equals(userId, event.userId) &&
               Objects.equals(payload, event.payload);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, type, userId, value, timestamp, payload);
    }
    
    @Override
    public String toString() {
        return "Event{" +
               "id='" + id + '\'' +
               ", type='" + type + '\'' +
               ", userId='" + userId + '\'' +
               ", value=" + value +
               ", timestamp=" + timestamp +
               ", payload='" + payload + '\'' +
               '}';
    }
}
