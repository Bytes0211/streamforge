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

/**
 * Custom Flink sink that writes data to MongoDB
 */
public class MongoDBSink extends RichSinkFunction<String> {
    
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSink.class);
    
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
            collection = database.getCollection("processed_data");
            
            LOG.info("Successfully connected to MongoDB");
        } catch (Exception e) {
            LOG.error("Failed to connect to MongoDB", e);
            throw e;
        }
    }
    
    @Override
    public void invoke(String value, Context context) throws Exception {
        try {
            Document doc = new Document()
                .append("data", value)
                .append("timestamp", System.currentTimeMillis())
                .append("processedAt", new java.util.Date());
            
            collection.insertOne(doc);
            LOG.debug("Inserted document into MongoDB: {}", value);
        } catch (Exception e) {
            LOG.error("Error writing to MongoDB", e);
            throw e;
        }
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        if (mongoClient != null) {
            mongoClient.close();
            LOG.info("Closed MongoDB connection");
        }
    }
}
