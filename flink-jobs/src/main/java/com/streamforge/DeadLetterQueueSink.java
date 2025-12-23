package com.streamforge;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Dead letter queue sink for failed/invalid events
 */
public class DeadLetterQueueSink extends RichSinkFunction<String> {
    
    private static final Logger LOG = LoggerFactory.getLogger(DeadLetterQueueSink.class);
    
    protected transient MongoClient mongoClient;
    protected transient MongoCollection<Document> collection;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // MongoDB connection string
        String connectionString = "mongodb://admin:password@mongodb:27017";
        
        try {
            mongoClient = MongoClients.create(connectionString);
            MongoDatabase database = mongoClient.getDatabase("streamforge");
            collection = database.getCollection("dead_letter_queue");
            
            LOG.info("Successfully connected to MongoDB for DLQ");
        } catch (Exception e) {
            LOG.error("Failed to connect to MongoDB for DLQ", e);
            throw e;
        }
    }
    
    @Override
    public void invoke(String value, Context context) throws Exception {
        try {
            Document doc = new Document()
                .append("rawData", value)
                .append("failedAt", new Date())
                .append("errorType", "PARSE_ERROR");
            
            collection.insertOne(doc);
            LOG.warn("Failed event sent to DLQ: {}", value);
        } catch (Exception e) {
            LOG.error("Error writing to DLQ", e);
            // Don't throw exception here - we don't want DLQ failures to crash the pipeline
        }
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        if (mongoClient != null) {
            mongoClient.close();
            LOG.info("Closed MongoDB DLQ connection");
        }
    }
}
