package com.streamforge.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;

public class EventTest {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Test
    public void testEventCreation() {
        Event event = new Event("id1", "click", "user123", 10.5, 
                               System.currentTimeMillis(), "test payload");
        
        assertEquals("id1", event.getId());
        assertEquals("click", event.getType());
        assertEquals("user123", event.getUserId());
        assertEquals(10.5, event.getValue(), 0.001);
        assertNotNull(event.getPayload());
    }
    
    @Test
    public void testEventValidation_Valid() {
        Event event = new Event("id1", "click", "user123", 10.5, 
                               System.currentTimeMillis(), "payload");
        assertTrue(event.isValid());
    }
    
    @Test
    public void testEventValidation_InvalidId() {
        Event event = new Event("", "click", "user123", 10.5, 
                               System.currentTimeMillis(), "payload");
        assertFalse(event.isValid());
        
        Event nullIdEvent = new Event(null, "click", "user123", 10.5, 
                                     System.currentTimeMillis(), "payload");
        assertFalse(nullIdEvent.isValid());
    }
    
    @Test
    public void testEventValidation_InvalidType() {
        Event event = new Event("id1", "", "user123", 10.5, 
                               System.currentTimeMillis(), "payload");
        assertFalse(event.isValid());
    }
    
    @Test
    public void testEventValidation_InvalidUserId() {
        Event event = new Event("id1", "click", "", 10.5, 
                               System.currentTimeMillis(), "payload");
        assertFalse(event.isValid());
    }
    
    @Test
    public void testEventValidation_InvalidValue() {
        Event event = new Event("id1", "click", "user123", -1.0, 
                               System.currentTimeMillis(), "payload");
        assertFalse(event.isValid());
    }
    
    @Test
    public void testEventValidation_InvalidTimestamp() {
        Event event = new Event("id1", "click", "user123", 10.5, 0, "payload");
        assertFalse(event.isValid());
    }
    
    @Test
    public void testJsonDeserialization() throws Exception {
        String json = "{\"id\":\"id1\",\"type\":\"click\",\"userId\":\"user123\"," +
                     "\"value\":10.5,\"timestamp\":1234567890,\"payload\":\"test\"}";
        
        Event event = objectMapper.readValue(json, Event.class);
        
        assertEquals("id1", event.getId());
        assertEquals("click", event.getType());
        assertEquals("user123", event.getUserId());
        assertEquals(10.5, event.getValue(), 0.001);
        assertEquals(1234567890, event.getTimestamp());
        assertEquals("test", event.getPayload());
        assertTrue(event.isValid());
    }
    
    @Test
    public void testJsonSerialization() throws Exception {
        Event event = new Event("id1", "click", "user123", 10.5, 
                               1234567890, "test");
        
        String json = objectMapper.writeValueAsString(event);
        assertTrue(json.contains("\"id\":\"id1\""));
        assertTrue(json.contains("\"type\":\"click\""));
        assertTrue(json.contains("\"userId\":\"user123\""));
        assertTrue(json.contains("\"value\":10.5"));
        assertTrue(json.contains("\"timestamp\":1234567890"));
    }
    
    @Test
    public void testEventEquals() {
        Event event1 = new Event("id1", "click", "user123", 10.5, 
                                1234567890, "test");
        Event event2 = new Event("id1", "click", "user123", 10.5, 
                                1234567890, "test");
        Event event3 = new Event("id2", "click", "user123", 10.5, 
                                1234567890, "test");
        
        assertEquals(event1, event2);
        assertNotEquals(event1, event3);
        assertEquals(event1.hashCode(), event2.hashCode());
    }
    
    @Test
    public void testEventToString() {
        Event event = new Event("id1", "click", "user123", 10.5, 
                               1234567890, "test");
        String str = event.toString();
        
        assertTrue(str.contains("id1"));
        assertTrue(str.contains("click"));
        assertTrue(str.contains("user123"));
        assertTrue(str.contains("10.5"));
    }
}
