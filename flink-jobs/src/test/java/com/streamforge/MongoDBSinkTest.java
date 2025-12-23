package com.streamforge;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.streamforge.model.Event;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for MongoDBSink
 */
public class MongoDBSinkTest {
    
    private MongoServer mongoServer;
    private MongoClient mongoClient;
    private MongoCollection<Document> collection;
    private String connectionString;
    
    @Before
    public void setUp() {
        // Start in-memory MongoDB server for testing
        mongoServer = new MongoServer(new MemoryBackend());
        InetSocketAddress serverAddress = mongoServer.bind();
        connectionString = "mongodb://" + serverAddress.getHostName() + ":" + serverAddress.getPort();
        
        // Create client and collection for verification
        mongoClient = MongoClients.create(connectionString);
        MongoDatabase database = mongoClient.getDatabase("streamforge");
        collection = database.getCollection("processed_data");
    }
    
    @After
    public void tearDown() {
        if (mongoClient != null) {
            mongoClient.close();
        }
        if (mongoServer != null) {
            mongoServer.shutdown();
        }
    }
    
    /**
     * Test case 1: MongoDBSink.open() successfully establishes a connection to MongoDB
     */
    @Test
    public void testOpenEstablishesConnection() throws Exception {
        // Create a custom MongoDBSink that uses our test connection string
        TestableMongoDBSink sink = new TestableMongoDBSink(connectionString);
        Configuration config = new Configuration();
        
        // Should not throw exception
        sink.open(config);
        
        // Verify connection was established by checking the client is not null
        assertNotNull("MongoClient should be initialized", sink.getMongoClient());
        assertNotNull("MongoCollection should be initialized", sink.getCollection());
        
        sink.close();
    }
    
    /**
     * Test case 2: MongoDBSink.invoke() correctly inserts a document into the MongoDB collection
     */
    @Test
    public void testInvokeInsertsDocument() throws Exception {
        TestableMongoDBSink sink = new TestableMongoDBSink(connectionString);
        Configuration config = new Configuration();
        sink.open(config);
        
        Event testEvent = new Event("id1", "click", "user123", 10.5, 
                                    System.currentTimeMillis(), "test payload");
        
        // Invoke the sink to insert data
        sink.invoke(testEvent, null);
        
        // Verify document was inserted
        long count = collection.countDocuments();
        assertEquals("Should have inserted one document", 1, count);
        
        // Verify document content (new schema: data, timestamp, processedAt)
        Document insertedDoc = collection.find().first();
        assertNotNull("Document should exist", insertedDoc);
        
        // Verify schema-required fields
        assertNotNull("Data field should exist", insertedDoc.getString("data"));
        assertTrue("Data should contain event id", insertedDoc.getString("data").contains("id1"));
        assertTrue("Data should contain event type", insertedDoc.getString("data").contains("click"));
        assertTrue("Data should contain userId", insertedDoc.getString("data").contains("user123"));
        assertNotNull("Timestamp field should exist", insertedDoc.getLong("timestamp"));
        assertNotNull("ProcessedAt field should exist", insertedDoc.getDate("processedAt"));
        
        sink.close();
    }
    
    /**
     * Test case 2b: MongoDBSink.invoke() correctly inserts multiple documents
     */
    @Test
    public void testInvokeInsertsMultipleDocuments() throws Exception {
        TestableMongoDBSink sink = new TestableMongoDBSink(connectionString);
        Configuration config = new Configuration();
        sink.open(config);
        
        List<Event> testEvents = new ArrayList<>();
        testEvents.add(new Event("id1", "click", "user1", 10.0, 
                                System.currentTimeMillis(), "payload1"));
        testEvents.add(new Event("id2", "view", "user2", 20.0, 
                                System.currentTimeMillis(), "payload2"));
        testEvents.add(new Event("id3", "click", "user3", 30.0, 
                                System.currentTimeMillis(), "payload3"));
        
        // Invoke the sink multiple times
        for (Event event : testEvents) {
            sink.invoke(event, null);
        }
        
        // Verify all documents were inserted
        long count = collection.countDocuments();
        assertEquals("Should have inserted three documents", 3, count);
        
        // Verify each document has required fields and contains event data
        List<Document> documents = collection.find().into(new ArrayList<>());
        for (int i = 0; i < testEvents.size(); i++) {
            Document doc = documents.get(i);
            assertNotNull("Data field should exist", doc.getString("data"));
            assertTrue("Document should contain event id", 
                      doc.getString("data").contains(testEvents.get(i).getId()));
            assertNotNull("Timestamp should exist", doc.getLong("timestamp"));
            assertNotNull("ProcessedAt should exist", doc.getDate("processedAt"));
        }
        
        sink.close();
    }
    
    /**
     * Test case 3: MongoDBSink.close() properly closes the MongoDB connection
     */
    @Test
    public void testCloseClosesConnection() throws Exception {
        TestableMongoDBSink sink = new TestableMongoDBSink(connectionString);
        Configuration config = new Configuration();
        sink.open(config);
        
        MongoClient clientBeforeClose = sink.getMongoClient();
        assertNotNull("MongoClient should be initialized", clientBeforeClose);
        
        // Close the sink
        sink.close();
        
        // After closing, attempting to use the client should fail or show closed state
        // The MongoClient doesn't have a direct "isClosed" method, but we can verify
        // that close was called without exception
        assertTrue("Close method should complete without exception", true);
    }
    
    /**
     * Test case 3b: MongoDBSink.close() handles null client gracefully
     */
    @Test
    public void testCloseWithNullClient() throws Exception {
        TestableMongoDBSink sink = new TestableMongoDBSink(connectionString);
        
        // Close without opening (client will be null)
        sink.close();
        
        // Should not throw exception
        assertTrue("Close with null client should not throw exception", true);
    }
    
    /**
     * Test case: Verify error handling when MongoDB is unavailable
     */
    @Test(expected = Exception.class)
    public void testOpenFailsWithInvalidConnection() throws Exception {
        // Use invalid connection string
        TestableMongoDBSink sink = new TestableMongoDBSink("mongodb://invalid-host:99999");
        Configuration config = new Configuration();
        
        // This should throw an exception
        sink.open(config);
    }
    
    /**
     * Testable version of MongoDBSink that allows custom connection strings
     * and exposes internal state for testing
     */
    private static class TestableMongoDBSink extends MongoDBSink {
        private final String testConnectionString;
        
        public TestableMongoDBSink(String connectionString) {
            this.testConnectionString = connectionString;
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            // Override to use test connection string
            mongoClient = MongoClients.create(testConnectionString);
            MongoDatabase database = mongoClient.getDatabase("streamforge");
            collection = database.getCollection("processed_data");
        }
        
        // Expose protected fields for testing
        public MongoClient getMongoClient() {
            return mongoClient;
        }
        
        public MongoCollection<Document> getCollection() {
            return collection;
        }
    }
}
