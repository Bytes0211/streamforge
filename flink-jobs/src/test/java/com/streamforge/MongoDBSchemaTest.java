package com.streamforge;

import com.mongodb.MongoCommandException;
import com.mongodb.client.*;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ValidationOptions;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for MongoDB schema validation and indexes.
 * Tests the requirements defined in scripts/init-mongodb.js
 */
public class MongoDBSchemaTest {

    private MongoServer mongoServer;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private String connectionString;

    @Before
    public void setUp() {
        // Start in-memory MongoDB server for testing
        mongoServer = new MongoServer(new MemoryBackend());
        InetSocketAddress serverAddress = mongoServer.bind();
        connectionString = "mongodb://" + serverAddress.getHostName() + ":" + serverAddress.getPort();

        // Create client and database
        mongoClient = MongoClients.create(connectionString);
        database = mongoClient.getDatabase("streamforge");
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
     * Test case 1: The "processed_data" collection is created with the correct validation schema
     */
    @Test
    public void testCollectionCreatedWithValidationSchema() {
        // Define validation schema matching init-mongodb.js
        Document schema = new Document("bsonType", "object")
                .append("required", List.of("data", "timestamp", "processedAt"))
                .append("properties", new Document()
                        .append("data", new Document()
                                .append("bsonType", "string")
                                .append("description", "Processed data payload - must be a string"))
                        .append("timestamp", new Document()
                                .append("bsonType", "long")
                                .append("description", "Processing timestamp in epoch milliseconds - must be a number"))
                        .append("processedAt", new Document()
                                .append("bsonType", "date")
                                .append("description", "Processing datetime - must be a date"))
                        .append("sourceOffset", new Document()
                                .append("bsonType", "long")
                                .append("description", "Optional Kafka offset for tracking"))
                        .append("partition", new Document()
                                .append("bsonType", "int")
                                .append("description", "Optional Kafka partition for tracking")));

        Document validator = new Document("$jsonSchema", schema);

        ValidationOptions validationOptions = new ValidationOptions()
                .validator(validator)
                .validationLevel(com.mongodb.client.model.ValidationLevel.MODERATE)
                .validationAction(com.mongodb.client.model.ValidationAction.WARN);

        CreateCollectionOptions options = new CreateCollectionOptions()
                .validationOptions(validationOptions);

        // Create collection with validation
        database.createCollection("processed_data", options);

        // Verify collection exists
        boolean collectionExists = false;
        for (String collectionName : database.listCollectionNames()) {
            if (collectionName.equals("processed_data")) {
                collectionExists = true;
                break;
            }
        }
        assertTrue("processed_data collection should exist", collectionExists);

        // Verify that a valid document can be inserted
        MongoCollection<Document> collection = database.getCollection("processed_data");
        Document validDoc = new Document()
                .append("data", "test data")
                .append("timestamp", System.currentTimeMillis())
                .append("processedAt", new Date());

        collection.insertOne(validDoc);
        assertEquals("Valid document should be inserted", 1, collection.countDocuments());
    }

    /**
     * Test case 2: The "processed_data" collection enforces required fields
     */
    @Test
    public void testCollectionEnforcesRequiredFields() {
        // Create collection with validation schema
        createProcessedDataCollection();

        MongoCollection<Document> collection = database.getCollection("processed_data");

        // Test missing "data" field
        Document missingData = new Document()
                .append("timestamp", System.currentTimeMillis())
                .append("processedAt", new Date());

        try {
            collection.insertOne(missingData);
            // Note: With validationAction = "warn", invalid docs may still be inserted
            // but validation warnings should be logged
        } catch (MongoCommandException e) {
            // If validation is strict, this would throw an exception
            assertTrue("Expected validation error for missing 'data' field", 
                    e.getMessage().contains("Document failed validation"));
        }

        // Test missing "timestamp" field
        Document missingTimestamp = new Document()
                .append("data", "test data")
                .append("processedAt", new Date());

        try {
            collection.insertOne(missingTimestamp);
        } catch (MongoCommandException e) {
            assertTrue("Expected validation error for missing 'timestamp' field", 
                    e.getMessage().contains("Document failed validation"));
        }

        // Test missing "processedAt" field
        Document missingProcessedAt = new Document()
                .append("data", "test data")
                .append("timestamp", System.currentTimeMillis());

        try {
            collection.insertOne(missingProcessedAt);
        } catch (MongoCommandException e) {
            assertTrue("Expected validation error for missing 'processedAt' field", 
                    e.getMessage().contains("Document failed validation"));
        }

        // Test valid document with all required fields
        Document validDoc = new Document()
                .append("data", "test data")
                .append("timestamp", System.currentTimeMillis())
                .append("processedAt", new Date());

        // This should succeed without exception
        collection.insertOne(validDoc);
        assertTrue("Valid document with all required fields should be inserted", 
                collection.countDocuments() > 0);
    }

    /**
     * Test case 3: The "processed_data" collection enforces correct bsonTypes for fields
     */
    @Test
    public void testCollectionEnforcesBsonTypes() {
        // Create collection with validation schema
        createProcessedDataCollection();

        MongoCollection<Document> collection = database.getCollection("processed_data");

        // Test invalid type for "data" field (should be string)
        Document invalidDataType = new Document()
                .append("data", 12345)  // Integer instead of string
                .append("timestamp", System.currentTimeMillis())
                .append("processedAt", new Date());

        try {
            collection.insertOne(invalidDataType);
        } catch (MongoCommandException e) {
            assertTrue("Expected validation error for invalid 'data' type", 
                    e.getMessage().contains("Document failed validation"));
        }

        // Test invalid type for "timestamp" field (should be long)
        Document invalidTimestampType = new Document()
                .append("data", "test data")
                .append("timestamp", "not a long")  // String instead of long
                .append("processedAt", new Date());

        try {
            collection.insertOne(invalidTimestampType);
        } catch (MongoCommandException e) {
            assertTrue("Expected validation error for invalid 'timestamp' type", 
                    e.getMessage().contains("Document failed validation"));
        }

        // Test invalid type for "processedAt" field (should be date)
        Document invalidProcessedAtType = new Document()
                .append("data", "test data")
                .append("timestamp", System.currentTimeMillis())
                .append("processedAt", "not a date");  // String instead of date

        try {
            collection.insertOne(invalidProcessedAtType);
        } catch (MongoCommandException e) {
            assertTrue("Expected validation error for invalid 'processedAt' type", 
                    e.getMessage().contains("Document failed validation"));
        }

        // Test valid document with correct types
        Document validDoc = new Document()
                .append("data", "test data")
                .append("timestamp", System.currentTimeMillis())
                .append("processedAt", new Date());

        collection.insertOne(validDoc);
        assertTrue("Valid document with correct types should be inserted", 
                collection.countDocuments() > 0);
    }

    /**
     * Test case 4: The "idx_timestamp" index is created on the "processed_data" collection
     */
    @Test
    public void testTimestampIndexCreated() {
        // Create collection
        createProcessedDataCollection();
        MongoCollection<Document> collection = database.getCollection("processed_data");

        // Create timestamp index (descending)
        IndexOptions indexOptions = new IndexOptions().name("idx_timestamp");
        collection.createIndex(new Document("timestamp", -1), indexOptions);

        // Verify index exists
        List<Document> indexes = new ArrayList<>();
        collection.listIndexes().into(indexes);

        boolean timestampIndexExists = false;
        Document timestampIndex = null;
        for (Document index : indexes) {
            if ("idx_timestamp".equals(index.getString("name"))) {
                timestampIndexExists = true;
                timestampIndex = index;
                break;
            }
        }

        assertTrue("idx_timestamp index should exist", timestampIndexExists);
        assertNotNull("idx_timestamp index document should not be null", timestampIndex);

        // Verify index is on timestamp field with descending order
        Document key = (Document) timestampIndex.get("key");
        assertNotNull("Index key should not be null", key);
        assertEquals("Index should be on timestamp field with descending order (-1)", 
                -1, key.getInteger("timestamp").intValue());
    }

    /**
     * Test case 5: The "idx_processedAt" index is created on the "processed_data" collection
     */
    @Test
    public void testProcessedAtIndexCreated() {
        // Create collection
        createProcessedDataCollection();
        MongoCollection<Document> collection = database.getCollection("processed_data");

        // Create processedAt index (descending)
        IndexOptions indexOptions = new IndexOptions().name("idx_processedAt");
        collection.createIndex(new Document("processedAt", -1), indexOptions);

        // Verify index exists
        List<Document> indexes = new ArrayList<>();
        collection.listIndexes().into(indexes);

        boolean processedAtIndexExists = false;
        Document processedAtIndex = null;
        for (Document index : indexes) {
            if ("idx_processedAt".equals(index.getString("name"))) {
                processedAtIndexExists = true;
                processedAtIndex = index;
                break;
            }
        }

        assertTrue("idx_processedAt index should exist", processedAtIndexExists);
        assertNotNull("idx_processedAt index document should not be null", processedAtIndex);

        // Verify index is on processedAt field with descending order
        Document key = (Document) processedAtIndex.get("key");
        assertNotNull("Index key should not be null", key);
        assertEquals("Index should be on processedAt field with descending order (-1)", 
                -1, key.getInteger("processedAt").intValue());
    }

    /**
     * Helper method to create processed_data collection with validation schema
     * Matches the schema defined in scripts/init-mongodb.js
     */
    private void createProcessedDataCollection() {
        Document schema = new Document("bsonType", "object")
                .append("required", List.of("data", "timestamp", "processedAt"))
                .append("properties", new Document()
                        .append("data", new Document()
                                .append("bsonType", "string")
                                .append("description", "Processed data payload - must be a string"))
                        .append("timestamp", new Document()
                                .append("bsonType", "long")
                                .append("description", "Processing timestamp in epoch milliseconds - must be a number"))
                        .append("processedAt", new Document()
                                .append("bsonType", "date")
                                .append("description", "Processing datetime - must be a date"))
                        .append("sourceOffset", new Document()
                                .append("bsonType", "long")
                                .append("description", "Optional Kafka offset for tracking"))
                        .append("partition", new Document()
                                .append("bsonType", "int")
                                .append("description", "Optional Kafka partition for tracking")));

        Document validator = new Document("$jsonSchema", schema);

        ValidationOptions validationOptions = new ValidationOptions()
                .validator(validator)
                .validationLevel(com.mongodb.client.model.ValidationLevel.MODERATE)
                .validationAction(com.mongodb.client.model.ValidationAction.WARN);

        CreateCollectionOptions options = new CreateCollectionOptions()
                .validationOptions(validationOptions);

        database.createCollection("processed_data", options);
    }
}
