package com.streamforge;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.streamforge.model.AggregatedMetrics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Custom Flink sink that writes aggregated metrics to MongoDB
 */
public class MongoDBMetricsSink extends RichSinkFunction<AggregatedMetrics> {
    
    private static final Logger LOG = LoggerFactory.getLogger(MongoDBMetricsSink.class);
    
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
            collection = database.getCollection("aggregated_metrics");
            
            LOG.info("Successfully connected to MongoDB for metrics");
        } catch (Exception e) {
            LOG.error("Failed to connect to MongoDB", e);
            throw e;
        }
    }
    
    @Override
    public void invoke(AggregatedMetrics metrics, Context context) throws Exception {
        try {
            Document doc = new Document()
                .append("userId", metrics.getUserId())
                .append("eventType", metrics.getEventType())
                .append("count", metrics.getCount())
                .append("sum", metrics.getSum())
                .append("avg", metrics.getAvg())
                .append("min", metrics.getMin())
                .append("max", metrics.getMax())
                .append("windowStart", new Date(metrics.getWindowStart()))
                .append("windowEnd", new Date(metrics.getWindowEnd()))
                .append("processedAt", new Date());
            
            collection.insertOne(doc);
            LOG.debug("Inserted metrics into MongoDB for user: {}, type: {}", 
                     metrics.getUserId(), metrics.getEventType());
        } catch (Exception e) {
            LOG.error("Error writing metrics to MongoDB", e);
            throw e;
        }
    }
    
    @Override
    public void close() throws Exception {
        super.close();
        if (mongoClient != null) {
            mongoClient.close();
            LOG.info("Closed MongoDB metrics connection");
        }
    }
}
